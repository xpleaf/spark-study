package cn.xpleaf.bigdata.spark.scala.core.p2

import cn.xpleaf.bigdata.spark.java.core.domain.{Score, Student}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

/**
  * 1、map：将集合中每个元素乘以7
    2、filter：过滤出集合中的奇数
    3、flatMap：将行拆分为单词
    4、sample：根据给定的随机种子seed，随机抽样出数量为frac的数据
    5、union：返回一个新的数据集，由原数据集和参数联合而成
    6、groupByKey：对数组进行 group by key操作
    7、reduceByKey：统计每个班级的人数
    8、join：打印关联的组合信息
    9、sortByKey：将学生身高进行排序
  */
object _02SparkTransformationOps {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName(_02SparkTransformationOps.getClass.getSimpleName)
        Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
        val sc = new SparkContext(conf)

        transformationOps9(sc)

        sc.stop()
    }

    /**
      * sortByKey：将学生身高进行（降序）排序
      *     身高相等，按照年龄排（升序）
      */
    def transformationOps9(sc: SparkContext): Unit = {
        val list = List(
            "1,李  磊,22,175",
            "2,刘银鹏,23,175",
            "3,齐彦鹏,22,180",
            "4,杨  柳,22,168",
            "5,敦  鹏,20,175"
        )
        val listRDD:RDD[String] = sc.parallelize(list)

        /*  // 使用sortBy操作完成排序
        val retRDD:RDD[String] = listRDD.sortBy(line => line, numPartitions = 1)(new Ordering[String] {
            override def compare(x: String, y: String): Int = {
                val xFields = x.split(",")
                val yFields = y.split(",")
                val xHgiht = xFields(3).toFloat
                val yHgiht = yFields(3).toFloat
                val xAge = xFields(2).toFloat
                val yAge = yFields(2).toFloat
                var ret = yHgiht.compareTo(xHgiht)
                if (ret == 0) {
                    ret = xAge.compareTo(yAge)
                }
                ret
            }
        } ,ClassTag.Object.asInstanceOf[ClassTag[String]])
        */
        // 使用sortByKey完成操作,只做身高降序排序
        val heightRDD:RDD[(String, String)] = listRDD.map(line => {
            val fields = line.split(",")
            (fields(3), line)
        })
        val retRDD:RDD[(String, String)] = heightRDD.sortByKey(ascending = false, numPartitions = 1)   // 需要设置1个分区，否则只是各分区内有序
        retRDD.foreach(println)

        // 使用sortByKey如何实现sortBy的二次排序？将上面的信息写成一个java对象，然后重写compareTo方法，在做map时，key就为该对象本身，而value可以为null

    }

    /**
      * 8、join：打印关联的组合信息
      * join(otherDataset, [numTasks]): 在类型为（K,V)和（K,W)类型的数据集上调用，返回一个（K,(V,W))对，每个key中的所有元素都在一起的数据集
      * 学生基础信息表和学生考试成绩表
      * stu_info(sid ,name, birthday, class)
      * stu_score(sid, chinese, english, math)
      *
      * *  Serialization stack:
	- object not serializable
        这种分布式计算的过程，一个非常重要的点，传递的数据必须要序列化

        通过代码测试，该join是等值连接(inner join)
        A.leftOuterJoin(B)
            A表所有的数据都包涵，B表中在A表没有关联的数据，显示为null
        之后执行一次filter就是join的结果
      */
    def transformationOps8(sc: SparkContext): Unit = {
        val infoList = List(
            "1,钟  潇,1988-02-04,bigdata",
            "2,刘向前,1989-03-24,linux",
            "3,包维宁,1984-06-16,oracle")
        val scoreList = List(
            "1,50,21,61",
            "2,60,60,61",
            "3,62,90,81",
            "4,72,80,81"
        )

        val infoRDD:RDD[String] = sc.parallelize(infoList)
        val scoreRDD:RDD[String] = sc.parallelize(scoreList)

        val infoPairRDD:RDD[(String, Student)] = infoRDD.map(line => {
            val fields = line.split(",")
            val student = new Student(fields(0), fields(1), fields(2), fields(3))
            (fields(0), student)
        })
        val scorePairRDD:RDD[(String, Score)] = scoreRDD.map(line => {
            val fields = line.split(",")
            val score = new Score(fields(0), fields(1).toFloat, fields(2).toFloat, fields(3).toFloat)
            (fields(0), score)
        })

        val joinedRDD:RDD[(String, (Student, Score))] = infoPairRDD.join(scorePairRDD)
        joinedRDD.foreach(t => {
            val sid = t._1
            val student = t._2._1
            val score = t._2._2
            println(sid + "\t" + student + "\t" + score)
        })

        println("=========================================")

        val leftOuterRDD:RDD[(String, (Score, Option[Student]))] = scorePairRDD.leftOuterJoin(infoPairRDD)
        leftOuterRDD.foreach(println)


    }

    /**
      * 7、reduceByKey：统计每个班级的人数
      * reduceByKey(func, [numTasks]): 在一个（K，V)对的数据集上使用，返回一个（K，V）对的数据集，
      * key相同的值，都被使用指定的reduce函数聚合到一起。和groupbykey类似，任务的个数是可以通过第二个可选参数来配置的。
      *
      * 需要注意的是还有一个reduce的操作，其为action算子，并且其返回的结果只有一个，而不是一个数据集
      * 而reduceByKey是一个transformation算子，其返回的结果是一个数据集
      */
    def transformationOps7(sc:SparkContext): Unit = {
        val list = List("hello you", "hello he", "hello me")
        val listRDD = sc.parallelize(list)
        val wordsRDD = listRDD.flatMap(line => line.split(" "))
        val pairsRDD:RDD[(String, Int)] = wordsRDD.map(word => (word, 1))
        val retRDD:RDD[(String, Int)] = pairsRDD.reduceByKey((v1, v2) => v1 + v2)

        retRDD.foreach(t => println(t._1 + "..." + t._2))
    }

    /**
      * 6、groupByKey：对数组进行 group by key操作
      * groupByKey([numTasks]): 在一个由（K,V）对组成的数据集上调用，返回一个（K，Seq[V])对的数据集。
      * 注意：默认情况下，使用8个并行任务进行分组，你可以传入numTask可选参数，根据数据量设置不同数目的Task
      * mr中：
      * <k1, v1>--->map操作---><k2, v2>--->shuffle---><k2, [v21, v22, v23...]>---><k3, v3>
      * groupByKey类似于shuffle操作
      *
      * 和reduceByKey有点类似，但是有区别，reduceByKey有本地的规约，而groupByKey没有本地规约，所以一般情况下，
      * 尽量慎用groupByKey，如果一定要用的话，可以自定义一个groupByKey，在自定义的gbk中添加本地预聚合操作
      */
    def transformationOps6(sc:SparkContext): Unit = {
        val list = List("hello you", "hello he", "hello me")
        val listRDD = sc.parallelize(list)
        val wordsRDD = listRDD.flatMap(line => line.split(" "))
        val pairsRDD:RDD[(String, Int)] = wordsRDD.map(word => (word, 1))
        pairsRDD.foreach(println)
        val gbkRDD:RDD[(String, Iterable[Int])] = pairsRDD.groupByKey()
        println("=============================================")
        gbkRDD.foreach(t => println(t._1 + "..." + t._2))
    }

    /**
      * 5、union：返回一个新的数据集，由原数据集和参数联合而成
      * union(otherDataset): 返回一个新的数据集，由原数据集和参数联合而成
      * 类似数学中的并集，就是sql中的union操作，将两个集合的所有元素整合在一块，包括重复元素
      */
    def transformationOps5(sc:SparkContext): Unit = {
        val list1 = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        val list2 = List(7, 8, 9, 10, 11, 12)
        val listRDD1 = sc.parallelize(list1)
        val listRDD2 = sc.parallelize(list2)
        val unionRDD = listRDD1.union(listRDD2)

        unionRDD.foreach(println)
    }

    /**
      * 4、sample：根据给定的随机种子seed，随机抽样出数量为frac的数据
      * sample(withReplacement, frac, seed): 根据给定的随机种子seed，随机抽样出数量为frac的数据
      * 抽样的目的：就是以样本评估整体
      * withReplacement:
      *     true：有放回的抽样
      *     false：无放回的抽样
      * frac：就是样本空间的大小，以百分比小数的形式出现，比如20%，就是0.2
      *
      * 使用sample算子计算出来的结果可能不是很准确，1000个数，20%，样本数量在200个左右，不一定为200
      *
      * 一般情况下，使用sample算子在做spark优化（数据倾斜）的方面应用最广泛
      */
    def transformationOps4(sc:SparkContext): Unit = {
        val list = 1 to 1000
        val listRDD = sc.parallelize(list)
        val sampleRDD = listRDD.sample(false, 0.2)

        sampleRDD.foreach(num => print(num + " "))
        println
        println("sampleRDD count: " + sampleRDD.count())
        println("Another sampleRDD count: " + sc.parallelize(list).sample(false, 0.2).count())
    }

    /**
      * 3、flatMap：将行拆分为单词
      * flatMap(func):类似于map，但是每一个输入元素，
      * 会被映射为0到多个输出元素（因此，func函数的返回值是一个Seq，而不是单一元素）
      */
    def transformationOps3(sc:SparkContext): Unit = {
        val list = List("hello you", "hello he", "hello me")
        val listRDD = sc.parallelize(list)
        val wordsRDD = listRDD.flatMap(line => line.split(" "))
        wordsRDD.foreach(println)
    }

    /**
      * 2、filter：过滤出集合中的奇数
      * filter(func): 返回一个新的数据集，由经过func函数后返回值为true的原元素组成
      *
      * 一般在filter操作之后都要做重新分区（因为可能数据量减少了很多）
      */
    def transformationOps2(sc:SparkContext): Unit = {
        val list = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        val listRDD = sc.parallelize(list)
        val retRDD = listRDD.filter(num => num % 2 == 0)
        retRDD.foreach(println)
    }

    /**
      * 1、map：将集合中每个元素乘以7
      * map(func):返回一个新的分布式数据集，由每个原元素经过func函数转换后组成
      */
    def transformationOps1(sc:SparkContext): Unit = {
        val list = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        val listRDD = sc.parallelize(list)
        val retRDD = listRDD.map(num => num * 7)
        retRDD.foreach(num => println(num))
    }
}
