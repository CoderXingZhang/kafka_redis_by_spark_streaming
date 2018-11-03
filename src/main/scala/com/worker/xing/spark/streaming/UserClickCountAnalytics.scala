package com.worker.xing.spark.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import net.sf.json.JSONObject
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.spark.streaming.Seconds
import redis.clients.jedis.JedisPool

object UserClickCountAnalytics {
  def main(args: Array[String]): Unit = {
    var masterUrl = "local[1]"
    if (args.length > 0){
      masterUrl = args(0)
    }

    val conf = new SparkConf().setMaster(masterUrl).setAppName("UserClickCountStat")
    val ssc = new StreamingContext(conf,Seconds(5))   // 批处理间隔（batch interval）

    // spark shell create ssc like this
    //val ssc = new StreamingContext(sc, Seconds(3))



    //Kafka configurations
    val topics = Set("user_events")
    val brokers = "192.168.3.210:9092"
    val kafkaParams = Map[String,String](
      "metadata.broker.list" -> brokers,"serializer.class" -> "kafka.serializer.StringEncoder"
    )

    val clickHashKey = "app::users::click"   //redis hash key

    //create a ditect stream
    val kafkaStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topics)

    val events = kafkaStream.flatMap( line => {
      val data = JSONObject.fromObject(line._2)
      Some(data)
    })

    //computer user click times
    val userClicks = events.map(x => (x.getString("uid"),x.getInt("click_count"))).reduceByKey(_+_)

    userClicks.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        /*
        * interal redis client for manageing redis connection
        * */
        object InternalRedisClient extends Serializable{
           private var pool:JedisPool = null

          def makePool(redisHost :String,redisPort:Int ,redisTimeout:Int,
                       maxTotal:Int,maxIdle:Int,minIdle:Int):Unit = {
            makePool(redisHost,redisPort,redisTimeout,maxTotal,maxIdle,minIdle,true,false,1000)
          }

          def makePool(redisHost:String,redisPort:Int,redisTimeOut:Int,
                       maxTotal:Int,maxIdle:Int,minIdle:Int,testOnBorrow:Boolean,
                       testOnReturn:Boolean,maxWaitMills:Long):Unit = {
            if (pool == null){
              val poolConfig = new GenericObjectPoolConfig()
              poolConfig.setMaxTotal(maxTotal)
              poolConfig.setMaxIdle(maxIdle)
              poolConfig.setMinIdle(minIdle)
              poolConfig.setTestOnBorrow(testOnBorrow)
              poolConfig.setTestOnReturn(testOnReturn)
              poolConfig.setMaxWaitMillis(maxWaitMills)

              pool = new JedisPool(poolConfig,redisHost,redisPort,redisTimeOut)
              val hook = new Thread{
                override def run(): Unit = pool.destroy()
              }
              sys.addShutdownHook(hook.run())
            }
          }

          def getPool:JedisPool = {
            assert(pool != null)
            pool
          }

          val maxTotal = 10
          val maxIdle = 10
          val minIdle = 1
          val redisHost = "192.168.3.210"
          val redisPort = 6379
          val redisTimeOut = 30000
          val dbIndex = 1
          InternalRedisClient.makePool(redisHost,redisPort,redisTimeOut,maxTotal,maxIdle,minIdle)
          val  jedis = InternalRedisClient.getPool.getResource
          jedis.select(dbIndex)

          partitionOfRecords.foreach(pair => {
            val uid = pair._1
            val clickCount = pair._2
            jedis.hincrBy(clickHashKey,uid,clickCount)
            InternalRedisClient.getPool.returnResource(jedis)
          })
        }
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
