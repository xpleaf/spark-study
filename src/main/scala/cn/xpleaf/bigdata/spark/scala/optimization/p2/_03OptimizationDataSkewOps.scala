package cn.xpleaf.bigdata.spark.scala.optimization.p2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
  * 通过抽样的方式找出发生数据倾斜的key
  * 然后再采用两阶段聚合的方式进行解决
  */
object _03OptimizationDataSkewOps {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName(_03OptimizationDataSkewOps.getClass.getSimpleName())
        val sc = new SparkContext(conf)
        Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
        Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)

        val list = List(
            "hello hello xiang qian liu",
            "hello liu liu liu double kill",
            "double kill liu liu liu",
            "liu liu liu wu kui shou",
            "ge liang hao liu liu liu")

        val listRDD = sc.parallelize(list)
        val wordsRDD = listRDD.flatMap(_.split(" "))

        // 找出需要过滤的数据量比较大的key
        val sampleWordsRDD = wordsRDD.sample(true, 0.4)
        val sampleSortedPairsRDD = sampleWordsRDD.map((_,1)).reduceByKey(_+_).map(t => (t._2, t._1)).sortByKey(false)
        // sampleSortedWordsRDD.foreach(println)    // (5,liu)........
        val filteredKeysArray = sampleSortedPairsRDD.map(t => t._2).take(2)
        println("需要进行两阶段聚合的key为：")
        filteredKeysArray.foreach(println)

        println("===============================================")
        // 将filteredKeysArray添加到广播变量中（也可以不添加）
        val filteredKeysArrayBC = sc.broadcast(filteredKeysArray)
        val pairsRDD = wordsRDD.map(word => {
            if(filteredKeysArrayBC.value.contains(word)) {  // 当前key是数据倾斜key，添加随机前缀
                val random = new Random()
                (random.nextInt(2) + "_" + word, 1)
            } else {
                (word, 1)
            }
        })
        println("==============添加随机前缀后的结果=================")
        pairsRDD.foreach(println)
        println("==============第一阶段的聚合(局部聚合)==============")
        val partRetRDD = pairsRDD.reduceByKey(_+_)
        partRetRDD.foreach(println)
        println("==============第二阶段的聚合(全局聚合)==============")
        val fulPairsRDD = partRetRDD.map(t => {
            val word = t._1
            val count = t._2
            if(word.contains("_")) {    // 说明是添加了前缀的key
                (word.split("_")(1), count)
            } else {
                (word, count)
            }
        })
        val retRDD = fulPairsRDD.reduceByKey(_+_)
        retRDD.foreach(println)


    }
}
