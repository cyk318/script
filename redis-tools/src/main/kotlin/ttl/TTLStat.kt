package ttl

import redis.clients.jedis.JedisPool
import redis.clients.jedis.params.ScanParams

fun main(args: Array<String>) {

    val jedisPool = JedisPool("tcp://10.200.16.83:6579")

    // 处理总数
    var total = 0
    // 最大过期时间的 key
    var keyForMaxTTL = "nil"
    // 最大过期时间
    var maxTTL = -1L

    jedisPool.resource.use { redis ->

        var cursor = ScanParams.SCAN_POINTER_START
        var index = 0

        do {
            val startTime = System.currentTimeMillis()

            // scan
            val scanResult = redis.scan(cursor, ScanParams().count(10000))
            val keys = scanResult.result
            total += keys.size
            // pipeline ttl
            redis.pipelined().use { pipeline ->
                // 记录下执行命令的顺序，方便后面获取过期时间最大的 key
                val recordKey = Array(keys.size) { "" }

                for (i in 0 ..< keys.size) {
                    pipeline.ttl(keys[i])
                    recordKey[i] = keys[i]
                }

                val ttlResult = pipeline.syncAndReturnAll()
                for (i in 0..< ttlResult.size) {
                    val curTTL = ttlResult[i] as Long
                    if (curTTL > maxTTL) {
                        keyForMaxTTL = recordKey[i]
                        maxTTL = curTTL
                    }
                }
            }
            cursor = scanResult.cursor

            println("----------------------------------------------------------------")
            println("已完成 $total ...   耗时: ${System.currentTimeMillis() - startTime}")
            println("本轮最佳 -> maxttl: $maxTTL  key: $keyForMaxTTL")
            println("----------------------------------------------------------------")
            println()

//            index++
//            if (index > 10) {
//                break
//            }
        } while (cursor != "0")

        println("-----------------------------end-------------------------------")
        println(keyForMaxTTL)
        println(maxTTL)
    }

}
