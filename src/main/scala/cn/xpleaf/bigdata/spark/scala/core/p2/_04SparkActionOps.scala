package cn.xpleaf.bigdata.spark.scala.core.p2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 学习Spark算子之action操作
  */
object _04SparkActionOps {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName(_04SparkActionOps.getClass.getSimpleName)
        Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
        val sc = new SparkContext(conf)

        reduceOps(sc)

        sc.stop()
    }

    def collectOps(sc:SparkContext): Unit = {
        val list = List("hello bo bo", "zhou xin xin", "hello song bo")
        val lineRDD = sc.parallelize(list)
        val wordsRDD = lineRDD.flatMap(line => line.split(" "))
        val pairsRDD = wordsRDD.map(word => (word, 1))
    }

    def reduceOps(sc:SparkContext): Unit = {
        val list = List("hello bo bo", "zhou xin xin", "hello song bo")
        val lineRDD = sc.parallelize(list)
        val wordsRDD = lineRDD.flatMap(line => line.split(" "))
        val pairsRDD = wordsRDD.map(word => (word, 1))

        val ret:(String, Int) = pairsRDD.reduce((t1, t2) => (t1._1, t1._2 + t2._2))
        println(ret)
    }
}
