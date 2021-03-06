package cn.xpleaf.bigdata.spark.scala.streaming.p2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 状态函数updateStateByKey
  *     更新key的状态(就是key对应的value)
  *
  * 通常的作用，计算某个key截止到当前位置的状态
  *     统计截止到目前为止的word对应count
  * 要想完成截止到目前为止的操作，必须将历史的数据和当前最新的数据累计起来，所以需要一个地方来存放历史数据
  * 这个地方就是checkpoint目录
  *
  * 演示Driver的HA
  *     要想执行Driver的HA，则必须要将Driver托管在Spark集群中，也就是说让Spark集群管理Driver
  *     必须要配置supervise
  */
object _02SparkStreamingDriverHAOps {
    def main(args: Array[String]): Unit = {
        if (args == null || args.length < 2) {
            System.err.println(
                """Parameter Errors! Usage: <hostname> <port>
                  |hostname: 监听的网络socket的主机名或ip地址
                  |port：    监听的网络socket的端口
                """.stripMargin)
            System.exit(-1)
        }
        Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
        val hostname = args(0).trim
        val port = args(1).trim.toInt
        val checkpoint = "hdfs://ns1/checkpoint/streaming/driver-ha"

        val conf = new SparkConf()
            .setAppName(_02SparkStreamingDriverHAOps.getClass.getSimpleName)

        // DriverHA
        def creatingFunc():StreamingContext = {
            val ssc = new StreamingContext(conf, Seconds(2))
            ssc.checkpoint(checkpoint)
            ssc
        }

        val ssc = StreamingContext.getOrCreate(checkpoint, creatingFunc)

        ssc.checkpoint(checkpoint)

        // 接收到的当前批次的数据
        val linesDStream:ReceiverInputDStream[String] = ssc.socketTextStream(hostname, port)
        // 这是记录下来的当前批次的数据
        val rbkDStream:DStream[(String, Int)] =linesDStream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)

        val usbDStream:DStream[(String, Int)]  = rbkDStream.updateStateByKey(updateFunc)

        usbDStream.print()

        ssc.start()
        ssc.awaitTermination()
        ssc.stop()  // stop中的boolean参数，设置为true，关闭该ssc对应的SparkContext，默认为false，只关闭自身
    }

    /**
      * @param seq 当前批次的key对应的数据
      * @param history 历史key对应的数据，可能有可能没有
      * @return
      */
    def updateFunc(seq: Seq[Int], history: Option[Int]): Option[Int] = {
        var sum = seq.sum
        if(history.isDefined) {
            sum += history.get
        }
        Option[Int](sum)
    }
}
