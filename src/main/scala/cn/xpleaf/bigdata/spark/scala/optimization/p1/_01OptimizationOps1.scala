package cn.xpleaf.bigdata.spark.scala.optimization.p1

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * scala开发调优
  */
object _01OptimizationOps1 {


    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName(_01OptimizationOps1.getClass.getSimpleName)
        val sc = new SparkContext(conf)

        optimizationOps2(sc)

        sc.stop()
    }

    def optimizationOps2(sc: SparkContext) = {
        val list = List("li zhi jie", "zhou xin xin li", "zhi jie jie", "zhou xin xin")
        val listRDD = sc.parallelize(list)
        val wordsRDD = listRDD.flatMap(_.split(" "))
        val pairsRDD = wordsRDD.mapPartitions(it => {
            println("===============================================")
            val ab = new ArrayBuffer[(String, Int)]()
            for(word <- it) {
                println("..." + word)
                ab.append((word, 1))
            }
            ab.iterator
        })
        pairsRDD.reduceByKey(_+_).foreach(println)
    }


    /**
      * 避免创建重复的rdd
      * 以wordcount案例为例，来计算出现单词的总个数，以及每个单词出现的次数
      */
    def optimizationOps1(sc:SparkContext): Unit = {
        val list = List("li zhi jie", "zhou xin xin li", "zhi jie jie", "zhou xin xin")
        val listRDD = sc.parallelize(list)
        val wordsRDD = listRDD.flatMap(_.split(" "))
        val pairsRDD = wordsRDD.map((_, 1))
        val retRDD = pairsRDD.reduceByKey(_+_)
        retRDD.foreach(println)
    }
}
