package cn.xpleaf.bigdata.spark.scala.optimization.p2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 通过抽样的方式找出发生数据倾斜的key
  * 然后再将其过滤掉
  */
object _02OptimizationDataSkewOps {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName(_02OptimizationDataSkewOps.getClass.getSimpleName())
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

        // 找出需要过滤的数据量比较大的前两个key
        val sampleWordsRDD = wordsRDD.sample(true, 0.4)
        val sampleSortedPairsRDD = sampleWordsRDD.map((_,1)).reduceByKey(_+_).map(t => (t._2, t._1)).sortByKey(false)
        // sampleSortedWordsRDD.foreach(println)    // (5,liu)........
        val filteredKeysArray = sampleSortedPairsRDD.map(t => t._2).take(2)
        println("需要过滤的key为：")
        filteredKeysArray.foreach(println)

        println("===============================================")
        val filteredWordsRDD = wordsRDD.filter(word => {
            !filteredKeysArray.contains(word)   // 返回结果为true时，则保留下来
        })
        filteredWordsRDD.map((_,1)).reduceByKey(_+_).foreach(println)

    }
}
