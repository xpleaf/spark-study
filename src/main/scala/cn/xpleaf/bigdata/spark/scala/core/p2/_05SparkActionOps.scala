package cn.xpleaf.bigdata.spark.scala.core.p2

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Spark算子操作之Action
  *     saveAsNewAPIHAdoopFile
  *     * saveAsHadoopFile
  * 和saveAsNewAPIHadoopFile的唯一区别就在于OutputFormat的不同
  * saveAsHadoopFile的OutputFormat使用的：org.apache.hadoop.mapred中的早期的类
  * saveAsNewAPIHadoopFile的OutputFormat使用的：org.apache.hadoop.mapreduce中的新的类
  * 使用哪一个都可以完成工作
  *
  * 前面在使用saveAsTextFile时也可以保存到hadoop文件系统中，注意其源代码也是使用上面的操作的
  *
  *   Caused by: java.net.UnknownHostException: ns1
	... 35 more
  找不到ns1，因为我们在本地没有配置，无法正常解析，就需要将hadoop的配置文件信息给我们加载进来
    hdfs-site.xml.heihei,core-site.xml.heihei
  */
object _05SparkActionOps {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName(_05SparkActionOps.getClass.getSimpleName)
        val sc = new SparkContext(conf)

        val list = List("hello you", "hello he", "hello me")
        val linesRDD = sc.parallelize(list)
        val wordsRDD = linesRDD.flatMap(line => line.split(" "))
        val pairsRDD = wordsRDD.map(word => (word, 1))
        val retRDD = pairsRDD.reduceByKey((v1, v2) => v1 + v2)

        retRDD.saveAsNewAPIHadoopFile(
            "hdfs://ns1/spark/action",      // 保存的路径
            classOf[Text],                      // 相当于mr中的k3
            classOf[IntWritable],               // 相当于mr中的v3
            classOf[TextOutputFormat[Text, IntWritable]]    // 设置(k3, v3)的outputFormatClass
        )

    }
}
