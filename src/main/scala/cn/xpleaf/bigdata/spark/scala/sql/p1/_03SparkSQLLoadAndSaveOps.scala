package cn.xpleaf.bigdata.spark.scala.sql.p1

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}

/**
  * SparkSQL关于加载数据和数据落地的各种实战操作
  */
object _03SparkSQLLoadAndSaveOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
        val conf = new SparkConf().setMaster("local[2]").setAppName(_01SparkSQLOps.getClass.getSimpleName)

        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)

        readOps(sqlContext)
//        writeOps(sqlContext)
        sc.stop()
    }

    /**
      * 在write结果到目录中的时候需要留意相关异常
      *     org.apache.spark.sql.AnalysisException: path file:/D:/data/spark/sql/people-1.json already exists
      * 如果还想使用该目录的话，就需要设置具体的保存模式SaveMode
      * ErrorIfExist
      *     默认的，目录存在，抛异常
      * Append
      *     追加
      * Ingore
      *     忽略，相当于不执行
      * Overwrite
      *     覆盖
      */
    def writeOps(sqlContext:SQLContext): Unit = {
        val df = sqlContext.read.json("D:/data/spark/sql/people.json")
        df.registerTempTable("people")
        val retDF = sqlContext.sql("select * from people where age > 20")
//        retDF.show()
        // 将结果落地
        //retDF.coalesce(1).write.mode(SaveMode.Overwrite).json("D:/data/spark/sql/people-1.json")
        // 落地到数据库
        val url = "jdbc:mysql://localhost:3306/test"
        val table = "people1"   // 会重新创建一张新表
        val properties = new Properties()
        properties.put("user", "root")
        properties.put("password", "root")
        retDF.coalesce(1).write.jdbc(url, table, properties)
    }

    /*
        // sparkSQL读数据
        // java.lang.RuntimeException: file:/D:/data/spark/sql/people.json is not a Parquet file
        sparkSQL使用read.load加载的默认文件格式为parquet（parquet.apache.org）
        加载其它文件格式怎么办？
            需要指定加载文件的格式.format("json")
     */
    def readOps(sqlContext:SQLContext): Unit = {
        //        val df = sqlContext.read.load("D:/data/spark/sql/users.parquet")
        //        val df = sqlContext.read.format("json").load("D:/data/spark/sql/people.json")
        //        val df = sqlContext.read.json("D:/data/spark/sql/people.json")
        val url = "jdbc:mysql://localhost:3306/test"
        val table = "people"
        val properties = new Properties()
        properties.put("user", "root")
        properties.put("password", "root")
        val df = sqlContext.read.jdbc(url, table, properties)

        df.show()
    }
}
