package cn.xpleaf.bigdata.spark.scala.streaming.p2

import com.sun.prism.PixelFormat.DataType
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
  * SparkStreaming应用案例
  * 需求：
  *     截止到目前为止的不同品类中的商家销售量topN
  * 原始数据：
  *
  *     uid   brand   category
  *
  *     001 mi moblie
  * 002 mi moblie
  * 003 mi moblie
  * 004 mi moblie
  * 005 huawei moblie
  * 006 huawei moblie
  * 分析：
  * 因为要计算截止到目前为止的数据统计量，所以必然需要有状态的key，就需要使用updateStateByKey这个算子
  * 我们可以通过算子updateStateByKey计算出品牌销售量，如何求取topN呢？
  **
  *最简单就是SparkSQL的row_number()开窗函数，
  **
  *SparkSQL用到的DataFrame是通过要么集合，要么rdd创建的，又得要是用transform操作
  *
  */
object _01SparkStreamingCaseOps {
    def main(args: Array[String]): Unit = {
        if (args == null || args.length < 2) {
            System.err.println(
                """Parameter Errors! Usage: <hostname> <port>
                  |hostname: 监听的网络socket的主机名或ip地址
                  |port：    监听的网络socket的端口
                """.stripMargin)
            System.exit(-1)
        }
        val hostname = args(0).trim
        val port = args(1).trim.toInt
        Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

        val conf = new SparkConf()
            .setAppName(_01SparkStreamingCaseOps.getClass.getSimpleName)
            .setMaster("local[2]")
        val ssc = new StreamingContext(conf, Seconds(10))
        ssc.checkpoint("hdfs://ns1/checkpoint/streaming/usb")

        val hiveContext = new HiveContext(ssc.sparkContext)

        // 数据格式：uid brand category
        val linesDStream:ReceiverInputDStream[String] = ssc.socketTextStream(hostname, port)
        /**
        计算品类对应品牌的销售量
                需要使用复合键：品牌+品类
          */
        val pairsDStream = linesDStream.map{case line => {
            val fields = line.split("\\|")   // 用"|"作分隔符，需要进行转义
            val uid = fields(0)
            val brand = fields(1)
            val category = fields(2)
            (category + "_" + brand, 1)
        }}

        val rbkDStream = pairsDStream.reduceByKey{case (v1, v2) => v1 + v2}

        // 截止到目前为止
        val usbDStream:DStream[(String, Int)] = rbkDStream
            .updateStateByKey[Int]((seq:Seq[Int], option:Option[Int]) => updateFunc(seq, option))

        // 求取topN如何计算
        usbDStream.foreachRDD(rdd => {
            if(!rdd.isEmpty()) {
                val rowRDD: RDD[Row] = rdd.map { case (key, count) => {
                    Row(key.split("_")(0), key.split("_")(1), count)
                }
                }
                val schema = StructType(List(
                    StructField("category", DataTypes.StringType, false),
                    StructField("brand", DataTypes.StringType, false),
                    StructField("count", DataTypes.IntegerType, false)
                ))
                val df = hiveContext.createDataFrame(rowRDD, schema)
                df.registerTempTable("t_goods_sales")
                val sql = "SELECT " +
                    "category, " +
                    "brand, " +
                    "count, " +
                    "ROW_NUMBER() OVER(PARTITION by category ORDER by count DESC) as rank " +
                    "FROM t_goods_sales " +
                    "HAVING rank < 4"
                val retDF = hiveContext.sql(sql)
                retDF.show()
            }
        })

        ssc.start()
        ssc.awaitTermination()
        ssc.stop()  // stop中的boolean参数，设置为true，关闭该ssc对应的SparkContext，默认为false，只关闭自身

    }

    /**
      * 状态函数
      */
    def updateFunc(seq: Seq[Int], option:Option[Int]): Option[Int] = {
        var sum = seq.sum + option.getOrElse(0)
        Option[Int](sum)
    }
}
