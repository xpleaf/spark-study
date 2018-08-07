package cn.xpleaf.bigdata.spark.scala.sql.p2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sql._
import org.elasticsearch.spark._

/**
  * Spark和ES的集成操作
  *     引入Spark和es的maven依赖
  *     elasticsearch-hadoop
  *     2.3.0
  *     将account.json加载到es的索引库spark/account
  *     可以参考官方文档：https://www.elastic.co/guide/en/elasticsearch/hadoop/2.3/spark.html
  */
object _02SparkElasticSearchOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
        val conf = new SparkConf()
                        .setAppName(_02SparkElasticSearchOps.getClass().getSimpleName)
                        .setMaster("local[2]")
        /**
          * Spark和es的集成配置
          */
        conf.set("es.index.auto.create", "true")
        conf.set("es.nodes", "uplooking01")
        conf.set("es.port", "9200")
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)

//        write2ES(sqlContext)

        readFromES(sc)

        sc.stop()
    }

    /**
      * 从es中读数据
      * （使用sparkContext进行操作）
      */
    def readFromES(sc:SparkContext): Unit = {
        val resources = "spark/account"  // 索引库/类型
        val jsonRDD = sc.esJsonRDD(resources)
        jsonRDD.foreach(println)
    }

    /**
      * 向es中写入数据
      * （使用sqlContext进行操作）
      */
    def write2ES(sqlContext:SQLContext): Unit = {
        val jsonDF = sqlContext.read.json("D:/data/spark/sql/account.json")
        val resources = "spark/account"  // 索引库/类型
        jsonDF.saveToEs(resources)
    }
}
