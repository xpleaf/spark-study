package cn.xpleaf.bigdata.spark.scala.core.p1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 基于Scala的WordCount统计
  *
  * java.net.UnknownHostException: ns1
  *
  * spark系统不认识ns1
  * 在spark的配置文件spark-defaults.conf中添加：
  *     spark.files /home/uplooking/app/hadoop/etc/hadoop/hdfs-site.xml,/home/uplooking/app/hadoop/etc/hadoop/core-site.xml
  */
object _01SparkWordCountOps {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setAppName(s"${_01SparkWordCountOps.getClass().getSimpleName}")
//            .setMaster("local")
        val sc = new SparkContext(conf)

//        val linesRDD:RDD[String] = sc.textFile("D:/data/spark/hello.txt")
        val linesRDD:RDD[String] = sc.textFile("hdfs://ns1/hello")
        /*val wordsRDD:RDD[String] = linesRDD.flatMap(line => line.split(" "))
        val parsRDD:RDD[(String, Int)] = wordsRDD.map(word => new Tuple2[String, Int](word, 1))
        val retRDD:RDD[(String, Int)] = parsRDD.reduceByKey((v1, v2) => v1 + v2)
        retRDD.collect().foreach(t => println(t._1 + "..." + t._2))*/

        // 更简洁的方式
        linesRDD.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).collect().foreach(t => println(t._1 + "..." + t._2))
        // collect不是必须要加的，但是如果在standalone的运行模式下，不加就看不到控制台的输出
        // 而在yarn运行模式下，是看不到输出的
        sc.stop()
    }
}
