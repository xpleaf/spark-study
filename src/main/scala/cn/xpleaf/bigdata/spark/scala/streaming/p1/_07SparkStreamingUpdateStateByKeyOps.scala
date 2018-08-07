package cn.xpleaf.bigdata.spark.scala.streaming.p1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
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
  */
object _07SparkStreamingUpdateStateByKeyOps {
    def main(args: Array[String]): Unit = {
        if (args == null || args.length < 2) {
            System.err.println(
                """Parameter Errors! Usage: <hostname> <port>
                  |hostname: 监听的网络socket的主机名或ip地址
                  |port：    监听的网络socket的端口
                """.stripMargin)
            System.exit(-1)
        }
        val hostname = args(0).trim
        val port = args(1).trim.toInt
        Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

        val conf = new SparkConf()
            .setAppName(_07SparkStreamingUpdateStateByKeyOps.getClass.getSimpleName)
            .setMaster("local[2]")
        val ssc = new StreamingContext(conf, Seconds(2))

        ssc.checkpoint("hdfs://ns1/checkpoint/streaming/usb")

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
