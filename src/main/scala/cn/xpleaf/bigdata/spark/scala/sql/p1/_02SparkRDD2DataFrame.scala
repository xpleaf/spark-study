package cn.xpleaf.bigdata.spark.scala.sql.p1

import java.util
import java.util.{Arrays, List}

import cn.xpleaf.bigdata.spark.java.sql.p1.Person
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * SparkRDD与DataFrame之间的转换操作
  * 1.通过反射的方式，将RDD转换为DataFrame
  * 2.通过动态编程的方式将RDD转换为DataFrame
  * 这里演示的是第1种
  */
object _02SparkRDD2DataFrame {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
        val conf = new SparkConf().setMaster("local[2]").setAppName(_01SparkSQLOps.getClass.getSimpleName)
        // 使用kryo的序列化方式
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        conf.registerKryoClasses(Array(classOf[Person]))

        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)

        val lines = sc.textFile("D:/data/spark/sql/sql-rdd-source.txt")
        val personRDD:RDD[Person] = lines.map(line => {
            val fields = line.split(",")
            val id = fields(0).trim.toInt
            val name = fields(1).trim
            val age = fields(2).trim.toInt
            val height = fields(3).trim.toDouble
            new Person(id, name, age, height)
        })

        val persons: util.List[Person] = util.Arrays.asList(
            new Person(1, "孙人才", 25, 179),
            new Person(2, "刘银鹏", 22, 176),
            new Person(3, "郭少波", 27, 178),
            new Person(1, "齐彦鹏", 24, 175))

//        val df:DataFrame = sqlContext.createDataFrame(persons, classOf[Person])   // 这种方式也可以
        val df:DataFrame = sqlContext.createDataFrame(personRDD, classOf[Person])

        df.show()

        sc.stop()

    }
}
