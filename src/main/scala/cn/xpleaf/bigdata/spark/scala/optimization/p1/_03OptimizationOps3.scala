package cn.xpleaf.bigdata.spark.scala.optimization.p1

import java.sql.DriverManager

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * scala开发调优
  */
object _03OptimizationOps3 {


    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName(_01OptimizationOps1.getClass.getSimpleName)
        val sc = new SparkContext(conf)
        Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
        Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)

        optimizationOps1(sc)

        sc.stop()
    }

    /**
      * repartition和coalesce
      *     其实没有区别，repartition调用的是coalesce(num, shuffle=true)的形式
      *     既可以增长分区，也可以减少分区，如果是要减少分区，建议使用coalesce，因为如果repartition之前有100个分区
      *     repartition之后有10个分区，可以不用走shuffle
      *     如果重分区前的分区是10，重分区后的分区个数为100，这样是要走shuffle的，因为这种操作要通过key的hashCode值 % partition数
      *     来确定其应该到哪个分区中去，走了shuffle。
      *
      *     如果执行filter之后，减少分区数量，使用coalesce的默认版本（shuffle = false）即可，不建议使用repartition
      *     （这样来分析，如果减少分区数量，不应该再使用shuffle把原来10个分区的数据拉取到新的5个分区中去了，
      *       可以直接在原来10个分区的基础上，使用其中5个，再让其它5个的数据拉取到其中即可，这样就不至于有shuffle操作。
      *     ）
      *
      *     repartitionAndSortWithinPartitions是对k-v键值对的操作
      */
    def optimizationOps1(sc:SparkContext): Unit = {
        val list = List("li zhi jie", "zhou xin xin li", "zhi jie jie", "zhou xin xin")
        val listRDD = sc.parallelize(list)
        val wordsRDD = listRDD.flatMap(_.split(" "))
        println("wordsRDD's count is :" + wordsRDD.count())
        println("wordsRDD's partition num is: " + wordsRDD.partitions.length)
//        val filteredRDD = wordsRDD.filter(word => word.length > 3).repartition(1)
        val filteredRDD = wordsRDD.filter(word => word.length > 3).coalesce(1)
        println("filteredRDD's count is :" + filteredRDD.count())
        println("filteredRDD's partition num is: " + filteredRDD.partitions.length)
//        filteredRDD.map((_,1)).repartitionAndSortWithinPartitions()
    }

}
