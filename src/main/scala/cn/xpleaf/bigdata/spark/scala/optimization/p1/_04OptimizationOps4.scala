package cn.xpleaf.bigdata.spark.scala.optimization.p1

import cn.xpleaf.bigdata.spark.java.core.domain.Phone
import org.apache.spark.{SparkConf, SparkContext}

object _04OptimizationOps4 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName(_04OptimizationOps4.getClass.getSimpleName)
        // 设置序列化器为KryoSerializer
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        // 注册要序列化的自定义类型
        conf.registerKryoClasses(Array(classOf[Phone]))
        val sc = new SparkContext(conf)

        val list = List("hello you", "hello he", "hello me")

        val listRDD = sc.parallelize(list)
        val wordsRDD = listRDD.flatMap(_.split(" "))
        val pairsRDD = wordsRDD.map((_, 1))
        val retRDD = pairsRDD.reduceByKey(_ + _)

        val phone = new Phone()
        phone.setName("Huawei")
        phone.setPrice(3688.0)

        val name = "Spark"

        retRDD.foreach(t => {
            println(t + "\t" + phone)
        })


        sc.stop()
    }
}
