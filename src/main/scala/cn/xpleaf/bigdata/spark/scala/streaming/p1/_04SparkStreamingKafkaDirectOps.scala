package cn.xpleaf.bigdata.spark.scala.streaming.p1

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Kafka和SparkStreaming基于Direct的模式集成
  *
  * 在公司中使用Kafka-Direct方式
  */
object _04SparkStreamingKafkaDirectOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

        val conf = new SparkConf()
            .setAppName(_04SparkStreamingKafkaDirectOps.getClass.getSimpleName)
            .setMaster("local[2]")
        val ssc = new StreamingContext(conf, Seconds(5))

//        ssc.checkpoint("hdfs://ns1/checkpoint/streaming/kafka")   // checkpoint文件也是可以保存到hdfs中的，不过必要性不大了，对于direct的方式来说

        val kafkaParams:Map[String, String] = Map("metadata.broker.list"-> "uplooking01:9092,uplooking02:9092,uplooking03:9092")
        val topics:Set[String] = Set("spark-kafka")
        val linesDStream:InputDStream[(String, String)] = KafkaUtils.
            // 参数分别为：key类型，value类型，key的解码器，value的解码器
            createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

        val retDStream = linesDStream.map(t => t._2).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)

        retDStream.print()

        ssc.start()
        ssc.awaitTermination()
        ssc.stop()
    }
}

