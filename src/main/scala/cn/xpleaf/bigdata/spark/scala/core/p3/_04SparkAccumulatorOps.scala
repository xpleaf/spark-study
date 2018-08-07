package cn.xpleaf.bigdata.spark.scala.core.p3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Spark共享变量之累加器Accumulator
  *
  * 需要注意的是，累加器的执行必须需要Action触发
  */
object _04SparkAccumulatorOps {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName(_01SparkPersistOps.getClass.getSimpleName())
        val sc = new SparkContext(conf)
        Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
        Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)

        // 要对这些变量都*7，同时统计能够被3整除的数字的个数
        val list = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13)

        val listRDD:RDD[Int] = sc.parallelize(list)
        var counter = 0
        val counterAcc = sc.accumulator[Int](0)
        val mapRDD = listRDD.map(num =>  {
            counter += 1
            if(num % 3 == 0) {
                counterAcc.add(1)
            }
            num * 7
        })
        // 下面这种操作又执行了一次RDD计算，所以可以考虑上面的方案，减少一次RDD的计算
        // val ret = mapRDD.filter(num => num % 3 == 0).count()
        mapRDD.foreach(println)
        println("counter===" + counter)
        println("counterAcc===" + counterAcc.value)
        sc.stop()
    }
}
