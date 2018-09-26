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
 * spark RDD的操作分为两种，第一为Transformation，第二为Action
 * 我们将Transformation称作转换算子，Action称作Action算子
 * Transformation算子常见的有：map flatMap reduceByKey groupByKey filter...
 * Action常见的有：foreach collect count save等等
 *
 * Transformation算子是懒加载的，其执行需要Action算子的触发
 * （可以参考下面的代码，只要foreach不执行，即使中间RDD的操作函数有异常也不会报错，因为其只是加载到内存中，并没有真正执行）
 */
public class _01SparkWordCountOps {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName(_01SparkWordCountOps.class.getSimpleName());
        /**
         * sparkConf中设置的master选择，
         * local
         *      local
         *          spark作业在本地执行，为该spark作业分配一个工作线程
         *      local[N]
         *          spark作业在本地执行，为该spark作业分配N个工作线程
         *      local[*]
         *          spark作业在本地执行，根据机器的硬件资源，为spark分配适合的工作线程，一般也就2个
         *      local[N, M]
         *          local[N, M]和上面最大的区别就是，当spark作业启动或者提交失败之后，可以有M次重试的机会，上面几种没有
         * standalone模式：
         *      就是spark集群中master的地址，spark://uplooking01:7077
         * yarn
         *      yarn-cluster
         *          基于yarn的集群模式，sparkContext的构建和作业的运行都在yarn集群中执行
         *      yarn-client
         *          基于yarn的client模式，sparkContext的构建在本地，作业的运行在集群
         *
         * mesos
         *      mesos-cluster
         *      mesos-client
         */
        String master = "local[*]";
        conf.setMaster(master);
        JavaSparkContext jsc = new JavaSparkContext(conf);
        Integer defaultParallelism = jsc.defaultParallelism();
        System.out.println("defaultParallelism=" + defaultParallelism);
        /**
         * 下面的操作代码，其实就是spark中RDD的DAG图
         */
        JavaRDD<String> linesRDD = jsc.textFile("D:/data/spark/hello.txt");
        System.out.println("linesRDD's partition size is: " + linesRDD.partitions().size());
        JavaRDD<String> wordsRDD = linesRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String line) throws Exception {
                // int i = 1 / 0;  // 用以验证Transformation算子的懒加载
                return Arrays.asList(line.split(" "));
            }
        });
        JavaPairRDD<String, Integer> pairRDD = wordsRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });
        
        // 上面是ShuffleMapStage
        // ----------------------分隔符----------------------
        // 即以pairRDD作为分隔符，前面的是ShuffleMapStage，后面的是ResultStage
        // ----------------------分隔符----------------------
        // 下面是ResultStage
        
        JavaPairRDD<String, Integer> retRDD = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        System.out.println("retRDD's partition size is: " + retRDD.partitions().size());
        retRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tuple) throws Exception {
                System.out.println(tuple._1 + "---" + tuple._2);
            }
        });
        jsc.close();
    }
}
