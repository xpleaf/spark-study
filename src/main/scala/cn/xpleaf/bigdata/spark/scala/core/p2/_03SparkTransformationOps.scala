package cn.xpleaf.bigdata.spark.scala.core.p2

import cn.xpleaf.bigdata.spark.java.core.domain.{Score, Student}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * spark的transformation操作：
  * aggregateByKey
  * combineByKey
  *
  * 使用combineByKey和aggregateByKey模拟groupByKey和reduceByKey
  *
  * 通过查看源码，我们发现aggregateByKey底层，还是combineByKey
  *
  * 问题：combineByKey和aggregateByKey的区别？
  * aggregateByKey是柯里化形式的，目前底层源码还没时间去分析，所知道的区别是这个
  */
object _03SparkTransformationOps {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName(_03SparkTransformationOps.getClass.getSimpleName)
        Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
        val sc = new SparkContext(conf)

//        combineByKey2GroupByKey(sc)
//        combineByKey2ReduceByKey(sc)
//        aggregateByKey2ReduceByKey(sc)
        aggregateByKey2GroupByKey(sc)

        sc.stop()
    }

    /**
      * 使用aggregateByKey模拟groupByKey
      */
    def aggregateByKey2GroupByKey(sc: SparkContext): Unit = {
        val list = List("hello bo bo", "zhou xin xin", "hello song bo")
        val lineRDD = sc.parallelize(list)
        val wordsRDD = lineRDD.flatMap(line => line.split(" "))
        val pairsRDD = wordsRDD.map(word => (word, 1))

        val retRDD:RDD[(String, ArrayBuffer[Int])] = pairsRDD.aggregateByKey(ArrayBuffer[Int]()) (  // 这里需要指定value的类型为ArrayBuffer[Int]()
          // 更正一下，其实上面的ArrayBuffer[Int]()只是初始化一个ArrayBuffer对象，这也就相当于是combineByKey中的createCombiner函数的操作
          // 所以其实这里可以先创建一个ArrayBuffer对象，再传到该位置中
            (part, num) => {
                part.append(num)
                part
            },
            (part1, part2) => {
                part1.++=(part2)
                part1
            }
        )

        retRDD.foreach(println)
    }

    /**
      * 使用aggregateByKey模拟reduceByKey
      *   def aggregateByKey[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U,
      combOp: (U, U) => U): RDD[(K, U)]
      (zeroValue: U)就对应的是combineByKey中的第一个函数的返回值
      seqOp 就对应的是combineByKey中的第二个函数，也就是mergeValue
      combOp 就对应的是combineByKey中的第三个函数，也就是mergeCombiners
      */
    def aggregateByKey2ReduceByKey(sc:SparkContext): Unit = {
        val list = List("hello bo bo", "zhou xin xin", "hello song bo")
        val lineRDD = sc.parallelize(list)
        val wordsRDD = lineRDD.flatMap(line => line.split(" "))
        val pairsRDD = wordsRDD.map(word => (word, 1))

        val retRDD:RDD[(String, Int)] = pairsRDD.aggregateByKey(0) (
            (partNum, num) => partNum + num,    // 也就是mergeValue
            (partNum1, partNum2) => partNum1 + partNum2 // 也就是mergeCombiners
        )

        retRDD.foreach(println)
    }

    /**
      * 使用reduceByKey模拟groupByKey
      */
    def combineByKey2ReduceByKey(sc:SparkContext): Unit = {
        val list = List("hello bo bo", "zhou xin xin", "hello song bo")
        val lineRDD = sc.parallelize(list)
        val wordsRDD = lineRDD.flatMap(line => line.split(" "))
        val pairsRDD = wordsRDD.map(word => (word, 1))

        /**
          * 对于createCombiner1   mergeValue1     mergeCombiners1
          * 代码的参数已经体现得很清楚了，其实只要理解了combineByKey模拟groupByKey的例子，这个就非常容易了
          */
        var retRDD:RDD[(String, Int)] = pairsRDD.combineByKey(createCombiner1, mergeValue1, mergeCombiners1)

        retRDD.foreach(println)
    }

    /**
      * reduceByKey操作，value就是该数值本身，则上面的数据会产生：
      * (hello, 1) (bo, 1)   (bo, 1)
      * (zhou, 1)  (xin, 1)  (xin, 1)
      * (hello, 1) (song, 1) (bo, 1)
      * 注意有别于groupByKey的操作，它是创建一个容器
      */
    def createCombiner1(num:Int):Int = {
        num
    }

    /**
      * 同一partition内，对于有相同key的，这里的mergeValue直接将其value相加
      * 注意有别于groupByKey的操作，它是添加到value到一个容器中
      */
    def mergeValue1(localNum1:Int, localNum2:Int): Int = {
        localNum1 + localNum2
    }

    /**
      * 将两个不同partition中的key相同的value值相加起来
      * 注意有别于groupByKey的操作，它是合并两个容器
      */
    def mergeCombiners1(thisPartitionNum1:Int, anotherPartitionNum2:Int):Int = {
        thisPartitionNum1 + anotherPartitionNum2
    }

    /**
      * 使用combineByKey模拟groupByKey
      */
    def combineByKey2GroupByKey(sc:SparkContext): Unit = {
        val list = List("hello bo bo", "zhou xin xin", "hello song bo")
        val lineRDD = sc.parallelize(list)
        val wordsRDD = lineRDD.flatMap(line => line.split(" "))
        val pairsRDD = wordsRDD.map(word => (word, 1))

        // 输出每个partition中的map对
        pairsRDD.foreachPartition( partition => {
            println("<=========partition-start=========>")
            partition.foreach(println)
            println("<=========partition-end=========>")
        })

        val gbkRDD:RDD[(String, ArrayBuffer[Int])] = pairsRDD.combineByKey(createCombiner, mergeValue, mergeCombiners)

        gbkRDD.foreach(println)

        // 如果要测试最后groupByKey的结果是在几个分区，可以使用下面的代码进行测试
        /*gbkRDD.foreachPartition(partition => {
            println("~~~~~~~~~~~~~~~~~~~~~~~~~~~")
            partition.foreach(println)
        })*/

    }

    /**
      * 初始化，将value转变成为标准的格式数据
      * 是在每个分区中进行的操作，去重后的key有几个，就调用次，
      * 因为对于每个key，其容器创建一次就ok了，之后有key相同的，只需要执行mergeValue到已经创建的容器中即可
      */
    def createCombiner(num:Int):ArrayBuffer[Int] = {
        println("----------createCombiner----------")
        ArrayBuffer[Int](num)
    }

    /**
      * 将key相同的value，添加到createCombiner函数创建的ArrayBuffer容器中
      * 一个分区内的聚合操作，将一个分区内key相同的数据，合并
      */
    def mergeValue(ab:ArrayBuffer[Int], num:Int):ArrayBuffer[Int] = {
        println("----------mergeValue----------")
        ab.append(num)
        ab
    }

    /**
      * 将key相同的多个value数组，进行整合
      * 分区间的合并操作
      */
    def mergeCombiners(ab1:ArrayBuffer[Int], ab2:ArrayBuffer[Int]):ArrayBuffer[Int] = {
        println("----------mergeCombiners----------")
        ab1 ++= ab2
        ab1
    }

}

/*
combineByKey模拟groupByKey的一个输出效果，可以很好地说明createCombiner、mergeValue和mergeCombiners各个阶段的执行时机：
<=========partition-start=========>
<=========partition-start=========>
(hello,1)
(zhou,1)
(bo,1)
(xin,1)
(bo,1)
(xin,1)
<=========partition-end=========>
(hello,1)
(song,1)
(bo,1)
<=========partition-end=========>
----------createCombiner----------
----------createCombiner----------
----------createCombiner----------
----------createCombiner----------
----------mergeValue----------
----------mergeValue----------
----------createCombiner----------
----------createCombiner----------
----------createCombiner----------
----------mergeCombiners----------
----------mergeCombiners----------
(song,ArrayBuffer(1))
(hello,ArrayBuffer(1, 1))
(bo,ArrayBuffer(1, 1, 1))
(zhou,ArrayBuffer(1))
(xin,ArrayBuffer(1, 1))
 */