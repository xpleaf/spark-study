package cn.xpleaf.bigdata.spark.scala.streaming.p1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
  * 使用Transformation之transform来完成在线黑名单过滤
  * 需求：
  *     将日志数据中来自于ip["27.19.74.143", "110.52.250.126"]实时过滤掉
  * 数据格式
  *     27.19.74.143##2016-05-30 17:38:20##GET /static/image/common/faq.gif HTTP/1.1##200##1127
  */
object _06SparkStreamingTransformOps {
    def main(args: Array[String]): Unit = {
        if (args == null || args.length < 2) {
            System.err.println(
                """Parameter Errors! Usage: <hostname> <port>
                  |hostname: 监听的网络socket的主机名或ip地址
                  |port：    监听的网络socket的端口
                """.stripMargin)
            System.exit(-1)
        }
        Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

        val conf = new SparkConf()
            .setAppName(_06SparkStreamingTransformOps.getClass.getSimpleName)
            .setMaster("local[2]")
        val ssc = new StreamingContext(conf, Seconds(2))

        val hostname = args(0).trim
        val port = args(1).trim.toInt

        //黑名单数据
        val blacklist = List(("27.19.74.143", true), ("110.52.250.126", true))
//        val blacklist = List("27.19.74.143", "110.52.250.126")
        val blacklistRDD:RDD[(String, Boolean)] = ssc.sparkContext.parallelize(blacklist)

        val linesDStream:ReceiverInputDStream[String] = ssc.socketTextStream(hostname, port)

        // 如果用到一个DStream和rdd进行操作，无法使用dstream直接操作，只能使用transform来进行操作
        val filteredDStream:DStream[String] = linesDStream.transform(rdd => {
            val ip2InfoRDD:RDD[(String, String)] = rdd.map{line => {
                (line.split("##")(0), line)
            }}
            /** A(M) B(N)两张表：
              * across join
              *     交叉连接，没有on条件的连接，会产生笛卡尔积(M*N条记录) 不能用
              * inner join
              *     等值连接，取A表和B表的交集，也就是获取在A和B中都有的数据，没有的剔除掉 不能用
              * left outer join
              *     外链接：最常用就是左外连接(将左表中所有的数据保留，右表中能够对应上的数据正常显示，在右表中对应不上，显示为null)
              *         可以通过非空判断是左外连接达到inner join的结果
              */
            val joinedInfoRDD:RDD[(String, (String, Option[Boolean]))] = ip2InfoRDD.leftOuterJoin(blacklistRDD)

            joinedInfoRDD.filter{case (ip, (line, joined)) => {
                joined == None
            }}//执行过滤操作
                .map{case (ip, (line, joined)) => line}
        })

        filteredDStream.print()

        ssc.start()
        ssc.awaitTermination()
        ssc.stop()  // stop中的boolean参数，设置为true，关闭该ssc对应的SparkContext，默认为false，只关闭自身
    }
}
