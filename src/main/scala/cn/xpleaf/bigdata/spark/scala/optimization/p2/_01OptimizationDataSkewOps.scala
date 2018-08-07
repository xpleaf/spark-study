package cn.xpleaf.bigdata.spark.scala.optimization.p2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 通过抽样的方式找出发生数据倾斜的key
  */
object _01OptimizationDataSkewOps {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName(_01OptimizationDataSkewOps.getClass.getSimpleName())
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

        val pairsRDD = listRDD.flatMap(_.split(" ")).map((_, 1))
        val samplePairsRDD = pairsRDD.sample(true, 0.4)
        samplePairsRDD.countByKey().foreach(println)

    }
}
