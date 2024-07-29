package offline

import redis.clients.jedis.JedisPool
import redis.clients.jedis.params.ScanParams


object OfflineConst {
    const val SCAN_COUNT = 100_0000
}

fun main() {
    val redis = JedisPool("tcp://10.200.16.83:6579").resource
    var total = 0


    val offlineMatch = "16:16.*:26"

    val keyCollections = mutableListOf<String>()

    redis.pipelined().use { pipeline ->
        var cursor = ScanParams.SCAN_POINTER_START

        do {
            val startTime = System.currentTimeMillis()

            val scanResult = redis.scan(cursor, ScanParams().count(OfflineConst.SCAN_COUNT).match(offlineMatch))
            cursor = scanResult.cursor
            val keys = scanResult.result
            if (keys.size != 0) {
                println("-----------------------------")
                println("发现 keys: ${keys}")
                println("-----------------------------")
                keyCollections.addAll(keys)
            }

            println("已扫描到 $total ...   耗时: ${System.currentTimeMillis() - startTime}")
        } while (cursor != "0")

        println()
        println("------------------扫描结束，key value 结果如下-----------------")

        for (i in 0 ..< keyCollections.size) {
            pipeline.hgetAll(keyCollections[i])
        }
        val hashResult = pipeline.syncAndReturnAll()
        println(hashResult)
    }
}