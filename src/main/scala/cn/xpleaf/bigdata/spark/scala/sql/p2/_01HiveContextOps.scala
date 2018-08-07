package cn.xpleaf.bigdata.spark.scala.sql.p2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * 通过创建HiveContext来操作Hive中表的数据
  * 数据源：
  * teacher_info.txt
  *     name(String)    height(double)
  *     zhangsan,175
  *     lisi,180
  *     wangwu,175
  *     zhaoliu,195
  *     zhouqi,165
  *     weiba,185
  *
  *     create table teacher_info(
  *     name string,
  *     height double
  *     ) row format delimited
  *     fields terminated by ',';
  *
  * teacher_basic.txt
  *     name(String)    age(int)    married(boolean)    children(int)
  *     zhangsan,23,false,0
  *     lisi,24,false,0
  *     wangwu,25,false,0
  *     zhaoliu,26,true,1
  *     zhouqi,27,true,2
  *     weiba,28,true,3
  *
  *     create table teacher_basic(
  *     name string,
  *     age int,
  *     married boolean,
  *     children int
  *     ) row format delimited
  *     fields terminated by ',';
  * *
  * 需求：
  *1.通过sparkSQL在hive中创建对应表，将数据加载到对应表
  *2.执行sparkSQL作业，计算teacher_info和teacher_basic的关联信息，将结果存放在一张表teacher中
  *
  * 在集群中执行hive操作的时候，需要以下配置：
  *     1、将hive-site.xml拷贝到spark/conf目录下，将mysql connector拷贝到spark/lib目录下
        2、在$SPARK_HOME/conf/spark-env.sh中添加一条记录
         export SPARK_CLASSPATH=$SPARK_CLASSPATH:$SPARK_HOME/lib/mysql-connector-java-5.1.39.jar
  */
object _01HiveContextOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
        val conf = new SparkConf()
//            .setMaster("local[2]")
            .setAppName(_01HiveContextOps.getClass.getSimpleName)

        val sc = new SparkContext(conf)
        val hiveContext = new HiveContext(sc)

        //创建teacher_info表
        hiveContext.sql("CREATE TABLE teacher_info(" +
            "name string, " +
            "height double) " +
            "ROW FORMAT DELIMITED " +
            "FIELDS TERMINATED BY ','")

        hiveContext.sql("CREATE TABLE teacher_basic(" +
            "name string, " +
            "age int, " +
            " married boolean, " +
            "children int) " +
            "ROW FORMAT DELIMITED " +
            "FIELDS TERMINATED BY ','")

        // 向表中加载数据
        hiveContext.sql("LOAD DATA LOCAL INPATH '/home/uplooking/data/hive/sql/teacher_info.txt' INTO TABLE teacher_info")
        hiveContext.sql("LOAD DATA LOCAL INPATH '/home/uplooking/data/hive/sql/teacher_basic.txt' INTO TABLE teacher_basic")

        //第二步操作 计算两张表的关联数据
        val joinDF = hiveContext.sql("SELECT " +
            "b.name, " +
            "b.age, " +
            "if(b.married, '已婚', '未婚') as married, " +
            "b.children, " +
            "i.height " +
            "FROM teacher_info i " +
            "INNER JOIN teacher_basic b ON i.name = b.name")

        joinDF.collect().foreach(println)

        joinDF.write.saveAsTable("teacher")


        sc.stop()
    }
}
