package merge

import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import redis.clients.jedis.params.ScanParams

// 总共合并个数
private var mergeTotal = 0
private var delTotal = 0

private val set = mutableSetOf<String>()
private var repeatTotal = 0

enum class RedisDataType {
    STRING,
    HASH,
    LIST,
    SET,
    ZSET,
    NONE,
    ;

    companion object {
        fun getType(type: String) = RedisDataType.entries.firstOrNull { it.name == type }
    }
}

data class RedisData (
    var key: String? = null,
    var value: Any? = null,
    var type: String? = null,
    var expire: Long? = null,
)

object MergeConst {
    const val SCAN_COUNT = 1_0000
    const val WRITE_COUNT = 10_0000
}

object CacheMerge {

    fun read(jedis: Jedis): List<RedisData> {
        val resultList = mutableListOf<RedisData>()
        var cursor = ScanParams.SCAN_POINTER_START
        var total = 0
        var num = 0

        println("<< 读取开始 >>")
        do {
            val startTime = System.currentTimeMillis()
            jedis.use { redis ->
                //1.scan 获取 key
                val scanResult = redis.scan(cursor, ScanParams().count(MergeConst.SCAN_COUNT))
                cursor = scanResult.cursor
                val keys = scanResult.result
                val dataList = Array(keys.size) { RedisData() }
                total += keys.size
                mergeTotal += keys.size

                //2.根据 key 的类型，获取所需要的值，组合成 RedisData
                // 1) 获取 type (注意大小写)
                redis.pipelined().use { pipeline ->
                    for (i in 0 ..< keys.size) {
                        pipeline.type(keys[i])
                        dataList[i].key = keys[i]
                    }
                    val types = pipeline.syncAndReturnAll()
                    for (i in 0 ..< keys.size) {
                        val type = types[i] as String
                        dataList[i].type = type.uppercase()
                    }
                }
                // 2) 获取 expire (大于 0 才设置)
                redis.pipelined().use { pipeline ->
                    for (i in 0 ..< keys.size) {
                        pipeline.ttl(keys[i])
                    }
                    val expires = pipeline.syncAndReturnAll()
                    for (i in 0 ..< keys.size) {
                        val expire = expires[i] as Long
                        if (expire > (60 * 60 * 24L)) { //大于 1 天时间才需要设置过期时间
                            dataList[i].expire = expire
                        } else if (expire == -1L) { //永不过期的 key
                            dataList[i].expire = null
                        } else { //其他一律按过期 key 来算
                            dataList[i].expire = -2L
                        }
                    }
                }
                // 3) 获取 value
                redis.pipelined().use { pipeline ->
                    //获取数据(Any)
                    for (i in 0 ..< keys.size) {
                        dataList[i].run {
                            when (RedisDataType.getType(type!!)) {
                                RedisDataType.STRING -> pipeline.get(key)
                                RedisDataType.HASH -> pipeline.hgetAll(key)
                                RedisDataType.LIST -> pipeline.lrange(key, 0, -1)
                                RedisDataType.SET -> pipeline.smembers(key)
                                RedisDataType.ZSET -> pipeline.zrangeWithScores(key, 0, -1)
                                RedisDataType.NONE -> {
                                    //过期的 key，也需要获取一下，否则 dataList 数据对应关系错乱
//                                    println("类型为 NONE: ${dataList[i]}")
                                    pipeline.get(key)
                                }
                                else -> throw IllegalArgumentException("暂不支持 $type 这种类型！data: $this")
                            }
                        }
                    }
                    val values = pipeline.syncAndReturnAll()
                    //注意为 null 的情况
                    for (i in 0 ..< keys.size) {
                        if (values[i] != null) {
                            dataList[i].value = values[i] as Any
                        } else {
                            dataList[i].value = null
                        }
                    }
                }
                resultList.addAll(dataList)
            }
            println("已读取 $total ...   耗时${System.currentTimeMillis() - startTime}" )

//            if (++num > 3) {
//                break
//            }

        } while (cursor != "0")
        println("<< 读取完成 >>")

        return resultList
    }

    fun write(redisDataList: List<RedisData>, redis: Jedis) {
        println("<< 写入开始 >>")
        redis.pipelined().use { pipeline ->
            val operator = PipelineWriteOperator()
            for (i in redisDataList.indices) {
                val data = redisDataList[i]

                //过期 key 就不用写入了
                if (data.expire == -2L) {
                    delTotal += 1
                    continue
                }

                //不通 redis 之间 key 重复，就不写了
                if (set.contains(data.key!!)) {
                    repeatTotal += 1
                    continue
                } else {
                    set.add(data.key!!)
                }

                // 写入管道
                try {
                    when (RedisDataType.getType(data.type!!)) {
                        RedisDataType.STRING -> operator.setStrategyApply(WriteStringStrategy())
                            .write(pipeline, data)

                        RedisDataType.HASH -> operator.setStrategyApply(WriteHashStrategy()).write(pipeline, data)
                        RedisDataType.LIST -> operator.setStrategyApply(WriteListStrategy()).write(pipeline, data)
                        RedisDataType.SET -> operator.setStrategyApply(WriteSetStrategy()).write(pipeline, data)
                        RedisDataType.ZSET -> operator.setStrategyApply(WriteZSetStrategy()).write(pipeline, data)
                        RedisDataType.NONE -> {} //刚好过期的 key 就不处理了
                        else -> throw IllegalArgumentException("暂不支持 ${data.type} 这种类型！data: $data")
                    }
                } catch (e: Exception) {
                    println("导致事故的数据 -> data: $data ")
                    e.printStackTrace()
                    return
                }

                // 模拟 AOF 批量刷新缓冲区
                if (i != 0 && i % MergeConst.WRITE_COUNT == 0) {
                    val startTime = System.currentTimeMillis()
                    pipeline.syncAndReturnAll()
                    println("已写入 $i ...   耗时${System.currentTimeMillis() - startTime}" )
                }
            }
            val startTime = System.currentTimeMillis()
            pipeline.sync() //把最后不到 10000 的数据刷新了
            println("最后写入... 耗时${System.currentTimeMillis() - startTime}" )
        }
        println("<< 写入完成 >>")

    }

}

fun main() {

    // 都合并到这个实例上
    val redisCyk = JedisPool("tcp://10.200.16.67:6390")

    //被合并的实例(将来这里是一个 list)
    val redisNotice = JedisPool("tcp://10.200.16.81:6379")
    val redisSession = JedisPool("tcp://10.200.16.75:6379")
    val redisUserCache = JedisPool("tcp://10.200.16.85:6679")
    val redisCache = JedisPool("tcp://10.200.16.67:6379")
    val redisTimeLimt = JedisPool("tcp://10.200.16.84:6379")
    val redisNzCache = JedisPool("tcp://10.200.16.73:6379")

    val destJedis = redisCyk.resource
    val redisInstances = listOf(
        redisNotice to "redis_notice",
        redisSession to "redis_session",
        redisUserCache to "redis_user-cache",
        redisCache to "redis_cache",
        redisTimeLimt to "redis-timelimt",
        redisNzCache to "redis_nz-cache"
    )

    for (instance in redisInstances) {
        val srcJedis = instance.first.resource

        println()
        println("---------------------- 开始处理: ${instance.second} ----------------------")
        // 1.从 被合并实例 中读取出数据(scan)
        val redisDataList = CacheMerge.read(srcJedis)
        // 2.写入 cyk实例
        CacheMerge.write(redisDataList, destJedis)
        println("---------------------- 处理完成: ${instance.second} ----------------------")
        println("_________")
        println("Note: 目前总共合并 ${mergeTotal - delTotal - repeatTotal}")
        println("_________")
        println()
    }

}