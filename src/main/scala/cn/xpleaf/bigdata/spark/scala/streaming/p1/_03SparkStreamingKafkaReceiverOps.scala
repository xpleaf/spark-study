package cn.xpleaf.bigdata.spark.scala.streaming.p1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Kafka和SparkStreaming基于Receiver的模式集成
  */
object _03SparkStreamingKafkaReceiverOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

        val conf = new SparkConf()
            .setAppName(_03SparkStreamingKafkaReceiverOps.getClass.getSimpleName)
            .setMaster("local[2]")
        val ssc = new StreamingContext(conf, Seconds(5))
//        ssc.checkpoint("hdfs://ns1/checkpoint/streaming/kafka")   // checkpoint文件保存到hdfs中
        ssc.checkpoint("file:///D:/data/spark/streaming/checkpoint/streaming/kafka")    // checkpoint文件保存到本地文件系统

        /**
          * 使用Kafka Receiver的方式，来创建的输入DStream，需要使用SparkStreaming提供的Kafka整合API
          * KafkaUtils
          */
        val zkQuorum = "uplooking01:2181,uplooking02:2181,uplooking03:2181"
        val groupId = "kafka-receiver-group-id"
        val topics:Map[String, Int] = Map("spark-kafka"->3)
        // ReceiverInputDStream中的key就是当前一条数据在kafka中的key，value就是该条数据对应的value
        val linesDStream:ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, zkQuorum, groupId, topics)

        val retDStream = linesDStream.map(t => t._2).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)

        retDStream.print()

        ssc.start()
        ssc.awaitTermination()
        ssc.stop()
    }
}

/*
所有kafka节点：kafka-server-start.sh -daemon config/server.properties
[uplooking@uplooking01 kafka]$ kafka-topics.sh --create --topic spark-kafka --zookeeper uplooking01:2181,uplooking02:2181,uplooking03:2181 --partitions 3 --replication-factor 3
Created topic "spark-kafka".
[uplooking@uplooking01 kafka]$ kafka-topics.sh --list --zookeeper uplooking01:2181,uplooking02:2181,uplooking03:2181
f-k-s
flume-kafka
hadoop
hbase
spark-kafka
 */
