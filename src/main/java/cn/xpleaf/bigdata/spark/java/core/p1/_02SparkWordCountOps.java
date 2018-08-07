package cn.xpleaf.bigdata.spark.java.core.p1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * spark Core 开发
 *
 * 基于Java
 * 计算国际惯例
 *
 * Spark程序的入口：
 *      SparkContext
 *          Java：JavaSparkContext
 *          scala：SparkContext
 *
 * D:/data\spark\hello.txt
 *
 * lambda表达式的版本
 */
public class _02SparkWordCountOps {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName(_02SparkWordCountOps.class.getSimpleName());
        String master = "local";
        conf.setMaster(master);
        JavaSparkContext jsc = new JavaSparkContext(conf);
        /**
         * 下面的操作代码，其实就是spark中RDD的DAG图
         * 现在使用lambda表达式，更加简单清晰
         */
        JavaRDD<String> linesRDD = jsc.textFile("D:/data/spark/hello.txt");
        JavaRDD<String> wordsRDD = linesRDD.flatMap(line -> {return Arrays.asList(line.split(" "));});
        JavaPairRDD<String, Integer> pairRDD = wordsRDD.mapToPair(word -> {return new Tuple2<String, Integer>(word, 1);});
        JavaPairRDD<String, Integer> retRDD = pairRDD.reduceByKey((v1, v2) -> {return v1 + v2;});
        retRDD.foreach(tuple -> {
            System.out.println(tuple._1 + "---" + tuple._2);
        });
        jsc.close();
    }
}
