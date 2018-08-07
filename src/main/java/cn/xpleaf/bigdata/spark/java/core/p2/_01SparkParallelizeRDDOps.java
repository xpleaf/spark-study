package cn.xpleaf.bigdata.spark.java.core.p2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * 基于并行化集合创建SparkRDD
 */
public class _01SparkParallelizeRDDOps {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName(_01SparkParallelizeRDDOps.class.getSimpleName());
        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("WARN");    // 设置日志级别，减少控制台输出
        List<String> list = Arrays.asList("hello you", "hello he", "hello me");

        JavaRDD<String> listRDD = jsc.parallelize(list);
        JavaRDD<String> wordsRDD = listRDD.flatMap(line -> {
            return Arrays.asList(line.split(" "));
        });
        JavaPairRDD<String, Integer> pairRDD = wordsRDD.mapToPair(word -> {
            return new Tuple2<String, Integer>(word, 1);
        });
        JavaPairRDD<String, Integer> retRDD = pairRDD.reduceByKey((v1, v2) -> {
            return v1 + v2;
        });
        retRDD.foreach(tuple -> {
            System.out.println(tuple._1 + "..." + tuple._2);
        });

        jsc.close();
    }
}
