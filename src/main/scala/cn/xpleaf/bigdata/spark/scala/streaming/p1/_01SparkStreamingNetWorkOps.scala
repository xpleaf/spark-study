package cn.xpleaf.bigdata.spark.scala.streaming.p1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object _01SparkStreamingNetWorkOps {
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

        val conf = new SparkConf()
                            .setAppName(_01SparkStreamingNetWorkOps.getClass.getSimpleName)
                            .setMaster("local[2]")
        val ssc = new StreamingContext(conf, Seconds(2))
        val hostname = args(0).trim
        val port = args(1).trim.toInt

        val linesDStream:ReceiverInputDStream[String] = ssc.socketTextStream(hostname, port)
        val wordsDStream:DStream[String] = linesDStream.flatMap({case line => line.split(" ")})
        val pairsDStream:DStream[(String, Integer)] = wordsDStream.map({case word => (word, 1)})
        val retDStream:DStream[(String, Integer)] = pairsDStream.reduceByKey{case (v1, v2) => v1 + v2}

        retDStream.print()

        ssc.start()
        ssc.awaitTermination()
        ssc.stop()  // stop中的boolean参数，设置为true，关闭该ssc对应的SparkContext，默认为false，只关闭自身
    }
}
