package cn.xpleaf.bigdata.spark.scala.sql.p2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import scala.util.Random

/**
  * SparkSQL在执行ByKey的过程出现dataskew怎么办？
  *     sparkCore中怎么做？
  *         两阶段
  *             局部聚合
  *                 +随机前缀的过程
  *             全局聚合
  *                 -随机前缀的过程
  */
object _07SparkSQLDataSkewOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
        val conf = new SparkConf()
            .setAppName(_07SparkSQLDataSkewOps.getClass().getSimpleName)
            .setMaster("local[2]")
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)
        val hiveContext = new HiveContext(sc)
        sqlContext.udf.register[String, String]("addRandomPrefix", word => addRandomPrefix(word))
        sqlContext.udf.register[String, String]("removeRandomPrefix", word => removeRandomPrefix(word))

        val list = List(
            "hello hello xiang qian liu",
            "hello liu liu liu double kill",
            "double kill liu liu liu",
            "liu liu liu wu kui shou",
            "ge liang hao liu liu liu")

        // 将RDD转换为DataFrame
        val rowRDD = sqlContext.sparkContext.parallelize(list).map(line => {
            Row(line)
        })
        val scheme = StructType(List(
            StructField("line", DataTypes.StringType, false)
        ))

        val df = sqlContext.createDataFrame(rowRDD, scheme)

        df.registerTempTable("test")

        // 最简单的操作
        /*
        val sql =  "select t.word, count(1) as count " +
            "from " +
            "(select " +
            "explode(split(line, ' ')) as word " +
            "from test) as t " +
            "group by t.word order by count desc"
            */

        // 第一步：拆分出每一个单词
        // val sql = "select explode(split(line, ' ')) as word from test"

        // 第二步：添加N以内的随机前缀 2
        // java.lang.UnsupportedOperationException  不支持再接一个查询，需要使用子查询
        //val sql = "select addRandomPrefix(explode(split(line, ' '))) as prefix_word from test"
        /*
        val sql = "SELECT " +
                        "addRandomPrefix(t1.word) as prefix_word " +
                    "FROM " +
                        "(SELECT EXPLODE(SPLIT(line, ' ')) as word FROM test) as t1"
        */

        // 第三步：统计有随机前缀的每个单词的次数--->局部统计
        /*
        val sql =   "SELECT " +
                        "t2.prefix_word, " +
                        "COUNT(t2.prefix_word) as prefix_count " +
                    "FROM" +
                            "(" +
                            "SELECT " +
                                "addRandomPrefix(t1.word) as prefix_word " +
                            "FROM " +
                                "(SELECT EXPLODE(SPLIT(line, ' ')) as word FROM test) as t1" +
                            ") as t2 GROUP by t2.prefix_word"
        */

        // 第四步：去掉随机前缀
        /*
        val sql =   "SELECT " +
                        "removeRandomPrefix(t3.prefix_word) as word, " +
                        "t3.prefix_count " +
                    "FROM " +
                            "(" +
                            "SELECT " +
                                "t2.prefix_word, " +
                                "COUNT(t2.prefix_word) as prefix_count " +
                            "FROM" +
                                    "(" +
                                    "SELECT " +
                                        "addRandomPrefix(t1.word) as prefix_word " +
                                    "FROM " +
                                        "(SELECT EXPLODE(SPLIT(line, ' ')) as word FROM test) as t1" +
                                    ") as t2 GROUP by t2.prefix_word" +
                            ") as t3"
        */

        // 第五步：最后的sum统计
        val sql =  "SELECT " +
                        "t4.word, " +
                        "sum(t4.prefix_count) as count " +
                    "FROM " +
                            "(" +
                            "SELECT " +
                                "removeRandomPrefix(t3.prefix_word) as word, " +
                                "t3.prefix_count " +
                            "FROM " +
                                    "(" +
                                    "SELECT " +
                                        "t2.prefix_word, " +
                                        "COUNT(t2.prefix_word) as prefix_count " +
                                    "FROM" +
                                            "(" +
                                            "SELECT " +
                                                "addRandomPrefix(t1.word) as prefix_word " +
                                            "FROM " +
                                                "(SELECT EXPLODE(SPLIT(line, ' ')) as word FROM test) as t1" +
                                            ") as t2 GROUP by t2.prefix_word" +
                                    ") as t3"   +
                            ") as t4 GROUP BY t4.word"



        sqlContext.sql(sql).show()
    }

    // 添加随机前缀的udf
    def addRandomPrefix(word:String):String = {
        val random = new Random()
        random.nextInt(2) + "_" + word
    }

    // 去除随机前缀的udf
    def removeRandomPrefix(word:String):String = {
        word.split("_")(1)
    }
}
