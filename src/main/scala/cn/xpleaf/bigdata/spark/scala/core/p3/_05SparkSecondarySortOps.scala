package cn.xpleaf.bigdata.spark.scala.core.p3

import cn.xpleaf.bigdata.spark.java.core.domain.SecondarySort
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

object _05SparkSecondarySortOps {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName(_05SparkSecondarySortOps.getClass.getSimpleName)
        val sc = new SparkContext(conf)

        val linesRDD = sc.textFile("D:/data/spark/secondsort.csv")

        /*
        val ssRDD:RDD[(SecondarySort, String)] = linesRDD.map(line => {
            val fields = line.split(" ")
            val first = Integer.valueOf(fields(0).trim())
            val second = Integer.valueOf(fields(1).trim())
            val ss = new SecondarySort(first, second)
            (ss, "")
        })

        // 第一种方式，使用元素具备比较性
        val sbkRDD:RDD[(SecondarySort, String)] = ssRDD.sortByKey(true, 1)

        sbkRDD.foreach{case (ss:SecondarySort, str:String) => { // 使用模式匹配的方式
            println(ss)
        }}
        */

        /*
        // 使用sortBy的第一种方式，基于原始的数据
        val retRDD = linesRDD.sortBy(line => line, numPartitions = 1)(new Ordering[String] {
            override def compare(x: String, y: String): Int = {
                val xFields = x.split(" ")
                val yFields = y.split(" ")

                var ret = xFields(0).toInt - yFields(0).toInt
                if(ret == 0) {
                    ret = yFields(1).toInt - xFields(1).toInt
                }
                ret
            }
        }, ClassTag.Object.asInstanceOf[ClassTag[String]])
        */

        // 使用sortBy的第二种方式，将原始数据做转换--->sortBy()第一个参数的作用，就是做数据的转换
        val retRDD:RDD[String] = linesRDD.sortBy(line => {
            // f: (T) => K
            // 这里T的类型为String，K是SecondarySort类型
            val fields = line.split(" ")
            val first = Integer.valueOf(fields(0).trim())
            val second = Integer.valueOf(fields(1).trim())
            val ss = new SecondarySort(first, second)
            ss
        }, true, 1)(new Ordering[SecondarySort] {
            override def compare(x: SecondarySort, y: SecondarySort): Int = {
                var ret = x.getFirst - y.getFirst
                if(ret == 0) {
                    ret = y.getSecond - x.getSecond
                }
                ret
            }
        }, ClassTag.Object.asInstanceOf[ClassTag[SecondarySort]])

        retRDD.foreach(println)

        sc.stop()
    }
}
