package cn.xpleaf.bigdata.spark.java.core.p2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class _02SparkTransformationOps {
    public static void main(String[] args) {
        transformationOps1();
    }

    /**
     * map(func):返回一个新的分布式数据集，由每个原元素经过func函数转换后组成
     * 1、map：将集合中每个元素乘以7
     */
    private static void transformationOps1() {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName(_02SparkTransformationOps.class.getSimpleName());
        JavaSparkContext jsc = new JavaSparkContext(conf);
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> listRDD = jsc.parallelize(list);
        JavaRDD<Integer> retRDD = listRDD.map(num -> {
            return num * 7;
        });
        retRDD.foreach(num -> System.out.println(num));

        jsc.close();
    }
}
