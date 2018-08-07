package cn.xpleaf.bigdata.spark.java.core.p3;

import cn.xpleaf.bigdata.spark.java.core.domain.SecondarySort;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Serializable;
import scala.Tuple2;

import java.util.Comparator;

/**
 * Java 版本的二次排序
 *   field_1' 'field_2(使用空格分割)
 *   20 21
 50 51
 50 52
 50 53
 50 54
 60 51
 60 53
 60 52
 60 56
 60 57
 70 58
 60 61
 70 54
 需求：首先按照第一列升序排序，如果第一列相等，按照第二列降序排序
 分析：要排序的话，使用sortByKey，也可以使用sortBy
 如果用sortByKey的话，只能按照key来排序，现在的是用第一列做key？还是第二列？
 根据需求，只能使用复合key（既包含第一列，也包含第二列），因为要进行比较，所以该复合key必须具备比较性，要么该操作提供一个比较器
 问题是查看该操作的时候，并没有给我们提供比较器，没得选只能让元素具备比较性

 使用自定义的对象 可以使用comprable接口
 */
public class _01SparkSecondarySortOps {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName(_01SparkSecondarySortOps.class.getSimpleName());
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> linesRDD = jsc.textFile("D:/data/spark/secondsort.csv");
        JavaPairRDD<SecondarySort, String> ssRDD = linesRDD.mapToPair(new PairFunction<String, SecondarySort, String>() {
            @Override
            public Tuple2<SecondarySort, String> call(String line) throws Exception {
                String[] fields = line.split(" ");
                int first = Integer.valueOf(fields[0].trim());
                int second = Integer.valueOf(fields[1].trim());
                SecondarySort ss = new SecondarySort(first, second);
                return new Tuple2<SecondarySort, String>(ss, "");
            }
        });
        /*
        // 第一种方式：使元素具备比较性
        JavaPairRDD<SecondarySort, String> sbkRDD = ssRDD.sortByKey(true, 1);   // 设置partition为1，这样数据才整体有序，否则只是partition中有序
        */

        /**
         * 第二种方式，提供比较器
         *      与前面方式相反，这次是：第一列降序，第二列升序
         */
        JavaPairRDD<SecondarySort, String> sbkRDD = ssRDD.sortByKey(new MyComparator<SecondarySort>() {
            @Override
            public int compare(SecondarySort o1, SecondarySort o2) {
                int ret = o2.getFirst() - o1.getFirst();
                if(ret == 0) {
                    ret = o1.getSecond() - o2.getSecond();
                }
                return ret;
            }
        }, true, 1);

        sbkRDD.foreach(new VoidFunction<Tuple2<SecondarySort, String>>() {
            @Override
            public void call(Tuple2<SecondarySort, String> tuple2) throws Exception {
                System.out.println(tuple2._1);
            }
        });

        jsc.close();
    }
}

/**
 * 做一个中间的过渡接口
 * 比较需要实现序列化接口，否则也会报异常
 * 是用到了适配器Adapter模式
 * 适配器模式(Adapter Pattern)是作为两个不兼容的接口之间的桥梁，这里就是非常好的体现了。
 */
interface MyComparator<T> extends Comparator<T>, Serializable{}