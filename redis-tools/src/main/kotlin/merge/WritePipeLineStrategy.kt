package merge

import redis.clients.jedis.Pipeline
import redis.clients.jedis.resps.Tuple

interface WritePipeLineStrategy {
    fun writePipeLine(pipeline: Pipeline, data: RedisData)
}

class WriteStringStrategy: WritePipeLineStrategy {

    override fun writePipeLine(pipeline: Pipeline, data: RedisData) {
        val key = data.key!!
        val value = data.value?.let { it as String }
        val expire = data.expire

        value?.let {
            pipeline.set(key, value)
            expire?.let {
                pipeline.expire(key, it)
            }
        }
    }

}

class WriteHashStrategy: WritePipeLineStrategy {

    override fun writePipeLine(pipeline: Pipeline, data: RedisData) {
        val key = data.key!!
        val value = data.value?.let { it as Map<String, String> }
        val expire = data.expire

        value?.let {
            pipeline.hset(key, value)
            expire?.let {
                pipeline.expire(key, it)
            }
        }
    }

}

class WriteListStrategy: WritePipeLineStrategy {

    override fun writePipeLine(pipeline: Pipeline, data: RedisData) {
        val key = data.key!!
        val value = data.value?.let { (it as List<String>).toTypedArray() }
        val expire = data.expire

        value?.let {
            pipeline.lpush(key, *value)
            expire?.let {
                pipeline.expire(key, it)
            }
        }
    }

}

class WriteSetStrategy: WritePipeLineStrategy {

    override fun writePipeLine(pipeline: Pipeline, data: RedisData) {
        val key = data.key!!
        val value = data.value?.let { (it as Set<String>).toTypedArray() }
        val expire = data.expire

        value?.let {
            pipeline.sadd(key, *value)
            expire?.let {
                pipeline.expire(key, it)
            }
        }
    }

}

class WriteZSetStrategy: WritePipeLineStrategy {

    override fun writePipeLine(pipeline: Pipeline, data: RedisData) {
        val key = data.key!!
        val values = data.value?.let {
            mutableMapOf<String, Double>().apply {
                val tuples = data.value as List<Tuple>
                tuples.forEach { tuple ->
                    this[tuple.element] = tuple.score
                }
            }
        }
        val expire = data.expire

        values?.let {
            pipeline.zadd(key, values)
            expire?.let {
                pipeline.expire(key, it)
            }
        }
    }

}

class PipelineWriteOperator {

    private lateinit var strategy: WritePipeLineStrategy

    fun setStrategyApply(strategy: WritePipeLineStrategy): PipelineWriteOperator {
        this.strategy = strategy
        return this
    }

    fun write(pipeline: Pipeline, data: RedisData) {
        this.strategy.writePipeLine(pipeline, data)
    }

}






