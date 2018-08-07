package cn.xpleaf.bigdata.spark.java.streaming.p1;

import com.google.common.base.Optional;
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
import java.util.List;

public class _02SparkStreamingUpdateStateByKeyOps {
    public static void main(String[] args) {
        if(args == null || args.length < 2) {
            System.err.println("Parameter Errors! Usage: <hostname> <port>");
            System.exit(-1);
        }
        Logger.getLogger("org.apache.spark").setLevel(Level.OFF);
        SparkConf conf = new SparkConf()
                .setAppName(_02SparkStreamingUpdateStateByKeyOps.class.getSimpleName())
                .setMaster("local[2]");

        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(2));
        jsc.checkpoint("hdfs://ns1/checkpoint/streaming/usb");

        String hostname = args[0].trim();
        int port = Integer.valueOf(args[1].trim());
        JavaReceiverInputDStream<String> lineDStream = jsc.socketTextStream(hostname, port);//默认的持久化级别：MEMORY_AND_DISK_SER_2


        JavaDStream<String> wordsDStream = lineDStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" "));
            }
        });

        JavaPairDStream<String, Integer> pairsDStream = wordsDStream.mapToPair(word -> {
            return new Tuple2<String, Integer>(word, 1);
        });

        JavaPairDStream<String, Integer> rbkDStream = pairsDStream.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        // 做历史的累计操作
        JavaPairDStream<String, Integer> usbDStream = rbkDStream.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            @Override
            public Optional<Integer> call(List<Integer> current, Optional<Integer> history) throws Exception {

                int sum = 0;
                for (int i : current) {
                    sum += i;
                }

                if (history.isPresent()) {
                    sum += history.get();
                }
                return Optional.of(sum);
            }
        });

        usbDStream.print();


        jsc.start();//启动流式计算
        jsc.awaitTermination();//等待执行结束
        jsc.close();
    }
}
