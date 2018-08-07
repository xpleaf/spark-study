package cn.xpleaf.bigdata.spark.scala.core.p3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 使用Spark广播变量
  *
  * 需求：
  *     用户表：
  *         id name age gender(0|1)
  *
  *     要求，输出用户信息，gender必须为男或者女，不能为0,1
  */
object _03SparkBroadcastOps {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName(_01SparkPersistOps.getClass.getSimpleName())
        val sc = new SparkContext(conf)
        Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
        Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)

        val userList = List(
            "001,刘向前,18,0",
            "002,冯  剑,28,1",
            "003,李志杰,38,0",
            "004,郭  鹏,48,2"
        )

        val genderMap = Map("0" -> "女", "1" -> "男")

        val genderMapBC:Broadcast[Map[String, String]] = sc.broadcast(genderMap)

        val userRDD = sc.parallelize(userList)
        val retRDD = userRDD.map(info => {
            val prefix = info.substring(0, info.lastIndexOf(","))   // "001,刘向前,18"
            val gender = info.substring(info.lastIndexOf(",") + 1)
            val genderMapValue = genderMapBC.value
            val newGender = genderMapValue.getOrElse(gender, "男")
            prefix + "," + newGender
        })
        retRDD.foreach(println)
        sc.stop()
    }
}
