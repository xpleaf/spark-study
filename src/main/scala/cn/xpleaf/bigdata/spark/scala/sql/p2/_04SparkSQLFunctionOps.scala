package cn.xpleaf.bigdata.spark.scala.sql.p2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * SparkSQL 内置函数操作
  */
object _04SparkSQLFunctionOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
        val conf = new SparkConf()
            .setAppName(_04SparkSQLFunctionOps.getClass().getSimpleName)
            .setMaster("local[2]")
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)

        /**
          * hive中的用户自定义函数UDF操作（即在SparkSQL中类比hive来进行操作，因为hive和SparkSQL都是交互式计算）
          * 1.创建一个普通的函数
          * 2.注册（在SqlContext中注册）
          * 3.直接使用即可
          *
          * 案例：创建一个获取字符串长度的udf
          */

        // 1.创建一个普通的函数
        def strLen(str:String):Int = str.length

        // 2.注册（在SqlContext中注册）
        sqlContext.udf.register[Int, String]("myStrLen", strLen)

        val list = List("Hello you", "Hello he", "Hello me")

        // 将RDD转换为DataFrame
        val rowRDD = sqlContext.sparkContext.parallelize(list).flatMap(_.split(" ")).map(word => {
            Row(word)
        })
        val scheme = StructType(List(
            StructField("word", DataTypes.StringType, false)
        ))
        val df = sqlContext.createDataFrame(rowRDD, scheme)

        df.registerTempTable("test")

        // 3.直接使用即可
        sqlContext.sql("select word, myStrLen(word) from test").show()


        sc.stop()
    }


}
