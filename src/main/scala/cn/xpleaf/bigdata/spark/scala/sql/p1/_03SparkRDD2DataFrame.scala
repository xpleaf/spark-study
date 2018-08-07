package cn.xpleaf.bigdata.spark.scala.sql.p1

import cn.xpleaf.bigdata.spark.java.sql.p1.Person
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * SparkRDD与DataFrame之间的转换操作
  * 1.通过反射的方式，将RDD转换为DataFrame
  * 2.通过动态编程的方式将RDD转换为DataFrame
  * 这里演示的是第2种
  */
object _03SparkRDD2DataFrame {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
        val conf = new SparkConf().setMaster("local[2]").setAppName(_01SparkSQLOps.getClass.getSimpleName)
        // 使用kryo的序列化方式
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        conf.registerKryoClasses(Array(classOf[Person]))

        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)

        val lines = sc.textFile("D:/data/spark/sql/sql-rdd-source.txt")
        val rowRDD:RDD[Row] = lines.map(line => {
            val fields = line.split(",")
            val id = fields(0).trim.toInt
            val name = fields(1).trim
            val age = fields(2).trim.toInt
            val height = fields(3).trim.toDouble
            Row(id, name, age, height)
        })

        val scheme = StructType(List(
            StructField("id", DataTypes.IntegerType, false),
            StructField("name", DataTypes.StringType, false),
            StructField("age", DataTypes.IntegerType, false),
            StructField("height", DataTypes.DoubleType, false)
        ))

        val df = sqlContext.createDataFrame(rowRDD, scheme)

        df.registerTempTable("person")
        sqlContext.sql("select max(age) as max_age, min(age) as min_age from person").show()

        sc.stop()

    }
}
