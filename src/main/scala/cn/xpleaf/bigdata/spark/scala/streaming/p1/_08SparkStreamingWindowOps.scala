package cn.xpleaf.bigdata.spark.scala.streaming.p1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *窗口函数window
  *   每隔多长时间(滑动频率slideDuration)统计过去多长时间(窗口长度windowDuration)中的数据
  * 需要注意的就是窗口长度和滑动频率
  * windowDuration = M*batchInterval，
    slideDuration = N*batchInterval
  */
object _08SparkStreamingWindowOps {
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
            .setAppName(_08SparkStreamingWindowOps.getClass.getSimpleName)
            .setMaster("local[2]")
        val ssc = new StreamingContext(conf, Seconds(2))

        // 接收到的当前批次的数据
        val linesDStream:ReceiverInputDStream[String] = ssc.socketTextStream(hostname, port)
        val pairsDStream:DStream[(String, Int)] =linesDStream.flatMap(_.split(" ")).map((_, 1))

        // 每隔4s，统计过去6s中产生的数据
        val retDStream:DStream[(String, Int)] = pairsDStream.reduceByKeyAndWindow(_+_, windowDuration = Seconds(6), slideDuration = Seconds(4))

        retDStream.print()

        ssc.start()
        ssc.awaitTermination()
        ssc.stop()  // stop中的boolean参数，设置为true，关闭该ssc对应的SparkContext，默认为false，只关闭自身
    }
}
