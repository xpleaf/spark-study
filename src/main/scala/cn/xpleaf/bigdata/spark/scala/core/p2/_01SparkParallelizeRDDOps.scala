package cn.xpleaf.bigdata.spark.scala.core.p2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 基于并行化集合创建SparkRDD
  */
object _01SparkParallelizeRDDOps {
    def main(args: Array[String]): Unit = {
        val conf:SparkConf = new SparkConf().setMaster("local[2]").setAppName(_01SparkParallelizeRDDOps.getClass.getSimpleName)

        // 屏蔽日志的其它方式，精确屏蔽某一个包产生的日志
        Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
        Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)

        val sc:SparkContext = new SparkContext(conf)
        val list = List("hello you", "hello he", "hello me")

        /*
        val listRDD:RDD[String] = sc.parallelize(list)
        val wordsRDD:RDD[String] = listRDD.flatMap(line => line.split(" "))
        val pairRDD:RDD[(String, Int)] = wordsRDD.map(word => new Tuple2[String, Int](word, 1))
        val retRDD:RDD[(String, Int)] = pairRDD.reduceByKey((v1, v2) => v1 + v2)
        retRDD.foreach(tuple => println(tuple._1 + "..." + tuple._2))
        */

        // 下面依然是一行代码搞定
        sc.parallelize(list).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).foreach(t => println(t._1 + "..." + t._2))

        sc.stop()
    }
}
