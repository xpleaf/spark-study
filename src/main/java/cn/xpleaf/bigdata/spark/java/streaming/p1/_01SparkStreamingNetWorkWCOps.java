package cn.xpleaf.bigdata.spark.java.streaming.p1;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * 使用Java开发SparkStreaming的第一个应用程序
 *
 * 用于监听网络socket中的一个端口，实时获取对应的文本内容
 * 计算文本内容中的每一个单词出现的次数
 */
public class _01SparkStreamingNetWorkWCOps {
    public static void main(String[] args) {
        if(args == null || args.length < 2) {
            System.err.println("Parameter Errors! Usage: <hostname> <port>");
            System.exit(-1);
        }
        Logger.getLogger("org.apache.spark").setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName(_01SparkStreamingNetWorkWCOps.class.getSimpleName())
        /*
         *  设置为local是无法计算数据，但是能够接收数据
         *  设置为local[2]是既可以计算数据，也可以接收数据
         *      当master被设置为local的时候，只有一个线程，且只能被用来接收外部的数据，所以不能够进行计算，如此便不会做对应的输出
         *      所以在使用的本地模式时，同时是监听网络socket数据，线程个数必须大于等于2
         */
                .setMaster("local[2]");
        /**
         * 第二个参数：Duration是SparkStreaming用于进行采集多长时间段内的数据将其拆分成一个个batch
         * 该例表示每隔2秒采集一次数据，将数据打散成一个个batch（其实就是SparkCore中的一个个RDD）
         */
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(2));

        String hostname = args[0].trim();
        int port = Integer.valueOf(args[1].trim());

        JavaReceiverInputDStream<String> lineDStream = jsc.socketTextStream(hostname, port);// 默认的持久化级别StorageLevel.MEMORY_AND_DISK_SER_2

        JavaDStream<String> wordsDStream = lineDStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String line) throws Exception {

                return Arrays.asList(line.split(" "));
            }
        });

        JavaPairDStream<String, Integer> pairsDStream = wordsDStream.mapToPair(word -> {
            return new Tuple2<String, Integer>(word, 1);
        });

        JavaPairDStream<String, Integer> retDStream = pairsDStream.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        retDStream.print();

        // 启动流式计算
        jsc.start();
        // 等待执行结束
        jsc.awaitTermination();
        System.out.println("结束了没有呀，哈哈哈~");
        jsc.close();
    }
}
