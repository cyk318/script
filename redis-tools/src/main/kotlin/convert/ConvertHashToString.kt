package convert

import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import redis.clients.jedis.params.ScanParams

data class HashData (
    val key: String,
    var value: Map<String, String>? = null, // field value
)

data class StringData (
    val key: String,
    val value: String,
)

object ConvertConst {
    const val SCAN_COUNT = 1000
    const val WRITE_COUNT = 100000
}

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

class ConvertHashToString {

    fun read(
        jedis: Jedis,
        result: MutableList<HashData>
    ) {
        var cursor = ScanParams.SCAN_POINTER_START
        var num = 0

        do {
            //管道操作遍历所有 key
            jedis.use { redis ->
                //scan 获取 key
                val startTime = System.currentTimeMillis()
                val scanResult = redis.scan(cursor, ScanParams().count(ConvertConst.SCAN_COUNT))
                cursor = scanResult.cursor

                val keys = scanResult.result

                val hashDataList = mutableListOf<HashData>()

                redis.pipelined().use { pipeline ->
                    //1.根据 type 获取 hash 类型的 key
                    for (i in 0 ..< keys.size) {
                        pipeline.type(keys[i])
                    }
                    val types = pipeline.syncAndReturnAll()
                    for (i in 0 ..< keys.size) {
                        val type = (types[i] as String).uppercase()
                        if (RedisDataType.HASH.name == type) {
                            hashDataList.add(HashData(key = keys[i]))
                        }
                        if (RedisDataType.NONE.name == type) {
                            println("发现 NONE 类型！key: ${keys[i]}")
                        }
                    }

                    //2.获取所有 hash 的 field-value
                    for (i in 0 ..< hashDataList.size) {
                        pipeline.hgetAll(hashDataList[i].key)
                    }
                    val maps = pipeline.syncAndReturnAll()
                    for (i in 0 ..< hashDataList.size) {
                        hashDataList[i].value = maps[i] as Map<String, String>
                    }
                }

                result.addAll(hashDataList)

                println("已读取 ${result.size} ...   耗时: ${System.currentTimeMillis() - startTime}")
            }

            if (++num > 1) {
                break
            }

        } while (cursor != "0")
    }

    fun convert(
        hashDataList: List<HashData>,
        result: MutableList<StringData>,
    ) {
        val startTime = System.currentTimeMillis()

        //1.遍历 hashDataList，将所有的 hash 都转化成 string (hashKey+field : value)
        for (hashData in hashDataList) {
            for ((field, value) in hashData.value!!) {
                result.add(
                    StringData(
                        key = "${hashData.key}.$field",
                        value = value
                    )
                )
            }
        }

        println()
        println("转化耗时: ${System.currentTimeMillis() - startTime}")
        println()
    }


}

fun main() {

    val redisSrc = JedisPool("tcp://10.200.16.83:6579").resource

    val convertHashToString = ConvertHashToString()

    //所有的 hash
    val hashDataList = mutableListOf<HashData>()
    val stringDataList = mutableListOf<StringData>()

    // 读取
    convertHashToString.read(redisSrc, hashDataList)
    println("总共读取: ${hashDataList.size}")
    // 转化
    convertHashToString.convert(hashDataList, stringDataList)
    println(stringDataList)
    // 写入
}