package cn.xpleaf.bigdata.spark.scala.optimization.p1

import java.sql.DriverManager

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * scala开发调优
  * 使用foreachPartitions代替foreach，通过向mysql写数据来说明
  */
object _02OptimizationOps2 {


    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName(_01OptimizationOps1.getClass.getSimpleName)
        val sc = new SparkContext(conf)
        Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
        Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)

        val list = List("li zhi jie", "zhou xin xin li", "zhi jie jie", "zhou xin xin")
        val listRDD = sc.parallelize(list)
        val wordsRDD = listRDD.flatMap(_.split(" "))
        val pairsRDD = wordsRDD.map((_, 1))
        val retRDD = pairsRDD.reduceByKey(_ + _)

        foreachOps4(sc)

        sc.stop()
    }

    /**
      * 分批次地往数据库中写数据，不要一下子将一个分区中的所有数据都写入数据库中
      * 可以设置一个策略，每隔多少条记录就执行一次批处理
      * 在本例中，总共有65条记录，每隔10条记录就执行一次批处理
      */
    def foreachOps4(sc:SparkContext): Unit = {
        val linesRDD = sc.textFile("D:/data/spark/10w_record.txt")
        val retRDD = linesRDD.flatMap(_.split(" ")).mapPartitions(it => {
            val ab = new ArrayBuffer[(String, Int)]()
            it.foreach(word => {
                ab.append((word, 1))
            })
            ab.iterator
        }).reduceByKey(_+_)
        retRDD.cache()

        retRDD.foreachPartition(it => {
            classOf[com.mysql.jdbc.Driver] // java中的Class.forName("com.mysql.jdbc.Driver")
            val user = "root"
            val password = "root"
            val url = "jdbc:mysql://localhost:3306/test"
            val connection = DriverManager.getConnection(url, user, password)
            val sql = "insert into result(word, count) values(?, ?)"
            val ps = connection.prepareStatement(sql)

            var times = 0
            it.foreach(t => {
                val word = t._1
                val count = t._2
                ps.setString(1, word)
                ps.setInt(2, count)
                ps.addBatch()   // 添加到批处理队列里
                times += 1
                if(times % 10 == 0) {
                    ps.executeBatch()
                }
            })
            if(times % 10 != 0) {   // 说明ps中还有数据
                ps.executeBatch()
            }
            ps.close()
            connection.close()
        })

    }

    /**
      * 比如咱们有10万条记录，要往数据库中写（前两种方式的比较）
      */
    def foreachOps3(sc:SparkContext): Unit = {
        val linesRDD = sc.textFile("D:/data/spark/10w_record.txt")
        val retRDD = linesRDD.flatMap(_.split(" ")).mapPartitions(it => {
            val ab = new ArrayBuffer[(String, Int)]()
            it.foreach(word => {
                ab.append((word, 1))
            })
            ab.iterator
        }).reduceByKey(_+_)
        retRDD.cache()

        var start = System.currentTimeMillis()
        foreachOps1(retRDD)
        println("foreachOps1消耗时间（foreach方式）：" + (System.currentTimeMillis() -start) + "ms") // 1556

        start = System.currentTimeMillis()
        //foreachOps2(retRDD)
        println("foreachOps2消耗时间（foreachPartition方式）：" + (System.currentTimeMillis() -start) + "ms")    // 1399

    }

    /**
      * 使用foreachPartition代替foreach操作，其中的操作结果集是一个分区中的所有数据，可以为该分区创建一个数据库的连接
      */
    def foreachOps2(retRDD: RDD[(String, Int)]): Unit = {
        retRDD.foreachPartition(it => {
            classOf[com.mysql.jdbc.Driver] // java中的Class.forName("com.mysql.jdbc.Driver")
            val user = "root"
            val password = "root"
            val url = "jdbc:mysql://localhost:3306/test"
            val connection = DriverManager.getConnection(url, user, password)
            val sql = "insert into result(word, count) values(?, ?)"
            val ps = connection.prepareStatement(sql)
            it.foreach(t => {
                val word = t._1
                val count = t._2
                ps.setString(1, word)
                ps.setInt(2, count)
                ps.addBatch()   // 添加到批处理队列里
            })
            ps.executeBatch()
            ps.close()
            connection.close()
        })
    }

    def foreachOps1(retRDD: RDD[(String, Int)]): Unit = {
        retRDD.foreach(t => {
            val word = t._1
            val count = t._2
            // 如果尝试在foreach外面初始化ps，然后在foreach里面再使用ps，会报下面的异常：
            // object not serializable (class: com.mysql.jdbc.JDBC42PreparedStatement, value: com.mysql.jdbc.JDBC42PreparedStatement@21618fa7:
            // insert into result(word, count) values(** NOT SPECIFIED **, ** NOT SPECIFIED **))
            // 原因很简单，foreach是在相对应的RDD中操作，而初始化是在Driver中的，这时就需要将ps传输到相对应的节点上
            // 其本身是没有序列化的，所以就会报这个异常
            // 正确的做法是将其放在foreach里面进行操作，相当于每次操作一条记录都会创建一个新的mysql连接
            classOf[com.mysql.jdbc.Driver] // java中的Class.forName("com.mysql.jdbc.Driver")
            val user = "root"
            val password = "root"
            val url = "jdbc:mysql://localhost:3306/test"
            val connection = DriverManager.getConnection(url, user, password)
            val sql = "insert into result(word, count) values(?, ?)"
            val ps = connection.prepareStatement(sql)
            ps.setString(1, word)
            ps.setInt(2, count)
            if (ps.execute()) {
                println("插入数据成功！")
            }
            ps.close()
            connection.close()
        })
    }

}
