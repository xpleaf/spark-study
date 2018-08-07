package cn.xpleaf.bigdata.spark.scala.optimization.p2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * 数据倾斜处理那些两个RDD进行join操作，但是其中一个RDD(RDD-1)中的个别key有数据倾斜，另外一个RDD(RDD-2)中的数据分布相对均匀
  * 适合本解决方案
  * 第一步：在RDD-1中找到发生数据倾斜的key
  * 第二步：
  *     将RDD-1拆分成两个子RDD
  *         RDD-1-sk
  *         RDD-1-2
  *     将RDD-2拆分成两个子RDD
  *         RDD-2-sk
  *         RDD-2-2
  * 第三步：
  *     将RDD-1-sk中的数据打上n以内的随机前缀
  *         RDD-1-sk-prefix
  *     将RDD-2-sk中的数据扩容n倍，同时依次打上0~n以内的随机前缀
  *         RDD-2-sk-prefix
  * 第四步：
  *     将有数据倾斜的rdd进行join操作
  *         RDD-1-sk-prefix.join(RDD-2-sk-prefix)得到新的结果rdd-sk-prefix-join
  *     将没有数据倾斜的rdd进行join操作
  *         RDD-1-2.join(RDD-2-2)得到新的结果rdd-join
  * 第五步：
  *     去掉rdd-sk-prefix-join前缀，成为rdd-sk-join
  * 第六步：
  *     将两部分的数据联合在一起，得到最终的结果
  *     rdd-sk-join.union(rdd-join)得到joinedRDD
  */
object _04OptimizationDataSkewOps {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName(_03OptimizationDataSkewOps.getClass.getSimpleName())
        val sc = new SparkContext(conf)
        Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
        Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)

        val list1 = List(
            "hello hello xiang qian liu",
            "hello liu liu liu double kill",
            "double kill liu liu liu",
            "liu liu liu wu kui shou",
            "ge liang hao liu liu liu")

        val list2 = List(
            "hello liu",
            "kill ge",
            "liu hao wu hello liu")

        // 第0步：基础准备操作
        val listRDD1 = sc.parallelize(list1)
        val listRDD2 = sc.parallelize(list2)

        val wordsRDD1 = listRDD1.flatMap(_.split(" "))  // 当前rdd中发生了data skew
        val wordsRDD2 = listRDD2.flatMap(_.split(" "))

        // 第一步：计算出发生数据倾斜的key
        val sampleRDD = wordsRDD1.sample(true, 0.4)
        val sbkSampleRDD = sampleRDD.map((_,1)).reduceByKey(_+_).map(t => (t._2, t._1)).sortByKey(false, 1).map(t => (t._2, t._1))
        val dataSkewKeys = sbkSampleRDD.take(2).map(t => t._1)  // 返回的是一个包含数据倾斜key的数组
        val dsBC = sc.broadcast(dataSkewKeys)

        /* 第二步：
         *     将RDD-1拆分成两个子RDD
         *         RDD-1-sk
         *         RDD-1-2
         *     将RDD-2拆分成两个子RDD
         *         RDD-2-sk
         *         RDD-2-2
         */
        val skRDD1 = wordsRDD1.filter(word => dsBC.value.contains(word))
        val commonRDD1 = wordsRDD1.filter(word => !dsBC.value.contains(word)).map((_,1))

        val skRDD2 = wordsRDD2.filter(word => dsBC.value.contains(word))
        val commonRDD2 = wordsRDD2.filter(word => !dsBC.value.contains(word)).map((_,1))

        /** 第三步：
          *   将RDD-1-sk中的数据打上n以内的随机前缀
          *       RDD-1-sk-prefix
          *   将RDD-2-sk中的数据扩容n倍，同时依次打上0~n以内的随机前缀
          *       RDD-2-sk-prefix(一条记录变多条记录，map做不到，只能用flatMap)
          */
        //skRDD1.foreach(println)   // 包含有13个liu和其它的某一个可能的key
        //println("===============")
        val prefixSKRDD1 = skRDD1.map(word => {
            val random = new Random()
            (random.nextInt(2) + "_" + word, 1)
        })
        //skRDD2.foreach(println)   // 包含有13个liu和其它的某一个可能的key
        //println("===============")
        val prefixSKRDD2 = skRDD2.flatMap(word => {
            val ab = new ArrayBuffer[(String, Int)]()
            for(i <- 0 until 2) {
                ab.append((i + "_" + word, 1))
            }
            ab
        })

        /** 第四步：
          *  将有数据倾斜的rdd进行join操作
          *     RDD-1-sk-prefix.join(RDD-2-sk-prefix)得到新的结果rdd-sk-prefix-join
          *  将没有数据倾斜的rdd进行join操作
          *     RDD-1-2.join(RDD-2-2)得到新的结果rdd-join
          */
        val prefixJoinRDD = prefixSKRDD1.join(prefixSKRDD2)
        val commonJoinRDD = commonRDD1.join(commonRDD2)

        // prefixJoinRDD.foreach(println)   // (0_liu,(1,1))    这样join之后的结果有39个，再一次深刻理解join操作

        /** 第五步：
          *   去掉rdd-sk-prefix-join前缀，成为rdd-sk-join
          */
        val joinSKRDD = prefixJoinRDD.map{case (word, count) => {
            (word.split("_")(1), count)
        }}

        //joinSKRDD.foreach(println)  // 包含39个(liu,(1,1))

        /** 第六步：
          * 将两部分的数据联合在一起，得到最终的结果
          */
        val retRDD = joinSKRDD.union(commonJoinRDD)

        retRDD.foreach(println)

    }
}
