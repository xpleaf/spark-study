package cn.xpleaf.bigdata.spark.scala.sql.p1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * SparkSQL基础操作学习
  * 操作SparkSQL的核心就是DataFrame，DataFrame带了一张内存中的二维表，包括元数据信息和表数据
  */
object _01SparkSQLOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
        val conf = new SparkConf().setMaster("local[2]").setAppName(_01SparkSQLOps.getClass.getSimpleName)
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)

        val df:DataFrame = sqlContext.read.json("D:/data/spark/sql/people.json")
        // 1.打印DF中所有的记录
        println("1.打印DF中所有的记录")
        df.show()   // 默认的输出表中数据的操作，相当于db中select * from t limit 20

        // 2.打印出DF中所有的schema信息
        println("2.打印出DF中所有的schema信息")
        df.printSchema()

        // 3.查询出name的列并打印出来 select name from t
        // df.select("name").show()
        println("3.查询出name的列并打印出来")
        df.select(new Column("name")).show()

        // 4.过滤并打印出年龄超过14岁的人
        println("4.过滤并打印出年龄超过14岁的人")
        df.select(new Column("name"), new Column("age")).where("age>14").show()

        // 5.给每个人的年龄都加上10岁
        println("5.给每个人的年龄都加上10岁")
        df.select(new Column("name"), new Column("age").+(10).as("10年后的年龄")).show()

        // 6.按照身高进行分组
        println("6.按照身高进行分组")   // select height, count(1) from t group by height;
        df.select(new Column("height")).groupBy(new Column("height")).count().show()

        // 注册表
        df.registerTempTable("people")
        // 执行sql操作
        var sql = "select height, count(1) from people group by height"
        sqlContext.sql(sql).show()

        sc.stop()


    }
}
