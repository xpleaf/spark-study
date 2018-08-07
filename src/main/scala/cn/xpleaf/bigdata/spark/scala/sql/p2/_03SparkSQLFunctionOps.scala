package cn.xpleaf.bigdata.spark.scala.sql.p2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * SparkSQL 内置函数操作
  */
object _03SparkSQLFunctionOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
        val conf = new SparkConf()
            .setAppName(_03SparkSQLFunctionOps.getClass().getSimpleName)
            .setMaster("local[2]")
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)

        val pdf = sqlContext.read.json("D:/data/spark/sql/people.json")
        pdf.show()

        pdf.registerTempTable("people")

        // 统计人数
        sqlContext.sql("select count(1) from people").show()
        // 统计最小年龄
        sqlContext.sql("select age, " +
            "max(age) as max_age, " +
            "min(age) as min_age, " +
            "avg(age) as avg_age, " +
            "count(age) as count " +
            "from people group by age order by age desc").show()


        sc.stop()
    }
}
