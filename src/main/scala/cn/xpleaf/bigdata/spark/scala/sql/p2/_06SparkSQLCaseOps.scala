package cn.xpleaf.bigdata.spark.scala.sql.p2

import java.util

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}

/**
  * 通过SparkSQL和SparkCore的综合案例，学习SparkSQL在企业中的使用
  * 同时学习掌握开窗函数row_number的使用
  *
  * 数据源
  * date        user    keyword  region  device   type
  * 2016-11-13	tom	china	beijing	pc	web
  * 2016-11-13	tom	news	tianjing	pc	web
  * 2016-11-13	john	china	beijing	pc	web
  * *
  * 需求：
  * 统计每天关键字被搜索最多的三个关键字(TopN)
  * 注意：一个用户搜索的若干关键字要进行去重
  * date    keyword      count
  * 
  * 分析：
  *     根据题意，我们需要对某一个日期中的用户的关键字进行去重，基于此可以通过reduceByKey算出每个关键字的次数
  *     在此基础之上来统计每天搜索前三的三个关键字
  *     注意：不管是使用哪一种去重方式，使用 用户+时间 或者 时间+关键字 都可以，通过复合key进行操作
  */
object _06SparkSQLCaseOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
        val conf = new SparkConf()
            .setAppName(_06SparkSQLCaseOps.getClass().getSimpleName)
            .setMaster("local[2]")
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)
        val hiveContext = new HiveContext(sc)
        
        val linesRdd = sc.textFile("D:/data/spark/sql/dailykey.txt")
        
        // (2016-11-13_china, tom)
        val dk2UserRDD:RDD[(String, String)] = linesRdd.map(line => {
            val fields = line.split("\t")
            val date = fields(0).trim
            val user = fields(1).trim
            val keyword = fields(2).trim
            (date + "_" + keyword, user)
        })

        /**
          * (2016-11-13_china, tom)
          * 要想统计出每天的每个关键字出现次数，最简单就是reduceByKey，问题是value是user，不是数字
          * 就可以使用将key对应的value拉取过来，在拉取的时候做去重，可以使用set
          * 
          * (2016-11-13_china, [tom, jack, jim])
          */
        val gbk2UserRDD = dk2UserRDD.combineByKey(createCombiner, mergeValue, mergeCombiners)
        gbk2UserRDD.foreach(println)
        println("=====================================")


        val dkCountRDD = gbk2UserRDD.map{case (date_keyword, users) => {
            (date_keyword, users.size())
        }}
        dkCountRDD.foreach(println)
        /*
        (2016-11-14_news,3)
        (2016-11-13_america,3)
        (2016-11-13_stopstar,1)
        (2016-11-14_topstar,3)
        (2016-11-14_sports,1)
        (2016-11-13_china,3)
        (2016-11-14_america,4)
        (2016-11-13_sports,1)
        (2016-11-14_stopstar,1)
        (2016-11-13_topstar,2)
        (2016-11-13_news,2)
        */
        println("=====================================")

        /**
          * date_key, count
          * 已经计算出每个key对应的次数，现在要求每一天的每个key的搜索次数的前N名
          * 分组TopN
          * 使用开窗函数row_number 必须掌握
          */

        val rowRDD:RDD[Row] = dkCountRDD.map{case (date_keyword, count) => {
            val fields = date_keyword.split("_")
            val date = fields(0)
            val keyword = fields(1)
            Row(date, keyword, count)
        }}

        val schema = StructType(List(
            StructField("data_date", DataTypes.StringType, false),
            StructField("keyword", DataTypes.StringType, false),
            StructField("count", DataTypes.IntegerType, false)
        ))

        val df = hiveContext.createDataFrame(rowRDD, schema)
        df.show()
        /*
        +----------+--------+-----+
        | data_date| keyword|count|
        +----------+--------+-----+
        |2016-11-14|    news|    3|
        |2016-11-13| america|    3|
        |2016-11-13|stopstar|    1|
        |2016-11-14| topstar|    3|
        |2016-11-14|  sports|    1|
        |2016-11-13|   china|    3|
        |2016-11-14| america|    4|
        |2016-11-13|  sports|    1|
        |2016-11-14|stopstar|    1|
        |2016-11-13| topstar|    2|
        |2016-11-13|    news|    2|
        +----------+--------+-----+
         */
        println("=====================================")

        df.registerTempTable("tmp_daily_keyword")

        val sql = "SELECT " +
                    "data_date, " +
                    "keyword, " +
                    "count, " +
                    "ROW_NUMBER() OVER(PARTITION by data_date ORDER by count DESC) as rank " +
                    "FROM tmp_daily_keyword " +
                    "HAVING rank < 4"

//        sqlContext.sql(sql)   // 开窗函数是hive提供的，所以需要使用下面的hiveContext
        hiveContext.sql(sql).show()


        sc.stop()

    }

    /**
      * 使用set，同时可以完成去重功能
      */
    def createCombiner(user:String):util.HashSet[String] = {
        val set = new util.HashSet[String]()
        set.add(user)
        set
    }
    
    def mergeValue(set:util.HashSet[String], value:String):util.HashSet[String] = {
        set.add(value)
        set
    }
    
    def mergeCombiners(set1:util.HashSet[String], set2:util.HashSet[String]):util.HashSet[String] = {
        set1.addAll(set2)
        set1
    }
}
