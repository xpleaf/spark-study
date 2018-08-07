package cn.xpleaf.bigdata.spark.scala.core.p3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * TopN问题的说明：
  *     TopN问题显然是可以使用action算子take来完成，但是因为take需要将所有数据都拉取到Driver上才能完成操作，
  *     所以Driver的内存压力非常大，不建议使用take.
  *
  * 这里要进行TopN问题的分析，数据及需求如下：
  * chinese ls 91
  * english ww 56
  * chinese zs 90
  * chinese zl 76
  * english zq 88
  * chinese wb 95
  * chinese sj 74
  * english ts 87
  * english ys 67
  * english mz 77
  * chinese yj 98
  * english gk 96
  *
  * 需求：排出每个科目的前三名
  *
  * 思路：先进行map操作转换为(subject, name + score)的元组
  * 再根据subject这个key进行groupByKey，这样就可以得到gbkRDD
  * 之后再对其进行map操作，在map操作中使用treeSet得到前三名（既能控制大小，又能进行排序）
  *
  * 问题：
  * 上面的方案在生产过程中慎用
  * 因为，执行groupByKey，会将key相同的数据都拉取到同一个partition中，再执行操作，
  * 拉取的过程是shuffle，是分布式性能杀手！再一个，如果key对应的数据过多，很有可能造成数据倾斜，或者OOM，
  * 那么就需要尽量的避免这种操作方式。
  * 那如何做到？可以参考MR中TopN问题的思想，MR中，是在每个map task中对数据进行筛选，虽然最后还是需要shuffle到一个节点上，但是数据量会大大减少。
  * Spark中参考其中的思想，就是可以在每个partition中对数据进行筛选，然后再对各个分区筛选出来的数据进行合并，再做一次排序，从而得到最终排序的结果。
  * 显然，这样就可以解决前面说的数据到同一个partition中导致数据量过大的问题！因为分区筛选的工作已经可以大大减少数据量。
  * 那么在Spark中有什么算子可以做到这一点呢？那就是combineByKey或者aggregateByKey，其具体的用法可以参考我前面的博客文章，这里我使用combineByKey来操作。
  */
object _06SparkTopNOps {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName(_06SparkTopNOps.getClass.getSimpleName())
        val sc = new SparkContext(conf)
        Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
        Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)
        Logger.getLogger("org.spark_project").setLevel(Level.OFF)

        // 1.转换为linesRDD
        val linesRDD:RDD[String] = sc.textFile("D:/data/spark/topn.txt")

        // 2.转换为pairsRDD
        val pairsRDD:RDD[(String, String)] = linesRDD.map(line => {
            val fields = line.split(" ")
            val subject = fields(0).trim()
            val name = fields(1).trim()
            val score = fields(2).trim()
            (subject, name + " " + score)   // ("chinese", "zs 90")
        })

        // 3.转换为gbkRDD
        val gbkRDD:RDD[(String, Iterable[String])] = pairsRDD.groupByKey()
        println("==========TopN前==========")
        gbkRDD.foreach(println)
        // (english,CompactBuffer(ww 56, zq 88, ts 87, ys 67, mz 77, gk 96))
        // (chinese,CompactBuffer(ls 91, zs 90, zl 76, wb 95, sj 74, yj 98))

        // 4.转换为retRDD
        val retRDD:RDD[(String, Iterable[String])] = gbkRDD.map(tuple => {
            var ts = new mutable.TreeSet[String]()(new MyOrdering())
            val subject = tuple._1          // chinese
            val nameScores = tuple._2       //  ("ls 91", "ww 56", "zs 90", ...)
            for(nameScore <- nameScores) {  // 遍历每一份成绩"ls 91"
                // 添加到treeSet中
                ts.add(nameScore)
                if(ts.size > 3) {   // 如果大小大于3，则弹出最后一份成绩
                    ts = ts.dropRight(1)
                }
            }
            (subject, ts)
        })

        println("==========TopN后==========")
        retRDD.foreach(println)

        sc.stop()
    }
}

// gbkRDD.map中用于排序的treeSet的排序比较规则，根据需求，应该为降序
class MyOrdering extends Ordering[String] {
    override def compare(x: String, y: String): Int = {
        // x或者y的格式为："zs 90"
        val xFields = x.split(" ")
        val yFields = y.split(" ")
        val xScore = xFields(1).toInt
        val yScore = yFields(1).toInt
        val ret = yScore - xScore
        ret
    }
}