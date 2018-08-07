package cn.xpleaf.bigdata.spark.scala.sql.p2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}

/**
  * 这两部分都比较重要：
  * 1.使用SparkSQL完成单词统计操作
  * 2.开窗函数使用
  */
object _05SparkSQLFunctionOps2 {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
        val conf = new SparkConf()
            .setAppName(_05SparkSQLFunctionOps2.getClass().getSimpleName)
            .setMaster("local[2]")
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)

        val list = List("Hello you", "Hello he", "Hello me")

        // 将RDD转换为DataFrame
        val rowRDD = sqlContext.sparkContext.parallelize(list).map(line => {
            Row(line)
        })
        val scheme = StructType(List(
            StructField("line", DataTypes.StringType, false)
        ))

        val df = sqlContext.createDataFrame(rowRDD, scheme)

        df.registerTempTable("test")

        df.show()

        // 执行wordcount
        val sql =  "select t.word, count(1) as count " +
                    "from " +
                        "(select " +
                            "explode(split(line, ' ')) as word " +
                        "from test) as t " +
                    "group by t.word order by count desc"
        sqlContext.sql(sql).show()

        sc.stop()
    }
}
