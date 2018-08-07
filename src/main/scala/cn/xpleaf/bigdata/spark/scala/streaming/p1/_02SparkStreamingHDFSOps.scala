package cn.xpleaf.bigdata.spark.scala.streaming.p1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * SparkStreaming监听hdfs的某一个目录的变化（新增文件）
  */
object _02SparkStreamingHDFSOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

        val conf = new SparkConf()
            .setAppName(_02SparkStreamingHDFSOps.getClass.getSimpleName)
            .setMaster("local[2]")
        val ssc = new StreamingContext(conf, Seconds(5))

//        val linesDStream:DStream[String] = ssc.textFileStream("hdfs://ns1/input/spark/streaming/")
        val linesDStream:DStream[String] = ssc.textFileStream("D:/data/spark/streaming")
        linesDStream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_).print()

        ssc.start()
        ssc.awaitTermination()
        ssc.stop()
    }
}
