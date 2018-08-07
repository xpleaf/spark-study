package cn.xpleaf.bigdata.spark.scala.core.p3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * 使用combineByKey算子来优化前面的TopN问题
  * 关于combineByKey算子的使用，可以参考我的博客文章，上面有非常详细的例子
  * 一定要掌握，因为非常重要
  */
object _07SparkTopNOps {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName(_07SparkTopNOps.getClass().getSimpleName())
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

        println("==========TopN前==========")
        pairsRDD.foreach(println)
        // (chinese,sj 74)
        // (chinese,ls 91)
        // (english,ts 87)
        // (english,ww 56)
        // (english,ys 67)
        // (chinese,zs 90)
        // (english,mz 77)
        // (chinese,zl 76)
        // (chinese,yj 98)
        // (english,zq 88)
        // (english,gk 96)
        // (chinese,wb 95)

        // 3.转换为cbkRDD
        val cbkRDD:RDD[(String, mutable.TreeSet[String])] = pairsRDD.combineByKey(createCombiner, mergeValue, mergeCombiners)

        println("==========TopN后==========")
        cbkRDD.foreach(println)
        // (chinese,TreeSet(yj 98, wb 95, ls 91))
        // (english,TreeSet(gk 96, zq 88, ts 87))
    }

    // 创建一个容器，这里返回一个treeSet，作为每个分区中相同key的value的容器
    def createCombiner(nameScore: String):mutable.TreeSet[String] = {
        // nameScore格式为："zs 90"
        // 指定排序规则MyOrdering，为降序排序
        val ts = new mutable.TreeSet[String]()(new MyOrdering())
        ts.add(nameScore)
        ts
    }

    // 合并分区中key相同的value，同时使用treeSet来进行排序
    def mergeValue(ts:mutable.TreeSet[String], nameScore:String):mutable.TreeSet[String] = {
        ts.add(nameScore)
        if(ts.size > 3) {   // 如果超过3个，删除一个再返回
            ts.dropRight(1) // scala中的集合进行操作后，本身不变，但是会返回一个新的集合
        }
        ts
    }

    // 合并不同分区中key相同的value集合，同时使用treeSet来进行排序
    def mergeCombiners(ts1:mutable.TreeSet[String], ts2:mutable.TreeSet[String]):mutable.TreeSet[String] = {
        var newTS = new mutable.TreeSet[String]()(new MyOrdering())
        // 将分区1中集合的value添加到新的treeSet中，同时进行排序和控制大小
        for(nameScore <- ts1) {
            newTS.add(nameScore)
            if(newTS.size > 3) {    // 如果数量大于3，则删除一个后再赋值给本身
                newTS = newTS.dropRight(1)
            }
        }
        // 将分区2中集合的value添加到新的treeSet中，同时进行排序和控制大小
        for(nameScore <- ts2) {
            newTS.add(nameScore)
            if(newTS.size > 3) {    // 如果数量大于3，则删除一个后再赋值给本身
                newTS = newTS.dropRight(1)
            }
        }
        newTS
    }


}
