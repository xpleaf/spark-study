package cn.xpleaf.bigdata.spark.java.sql.p1;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.util.Arrays;
import java.util.List;

/**
 * SparkRDD与DataFrame之间的转换操作
 * 1.通过反射的方式，将RDD转换为DataFrame
 * 2.通过动态编程的方式将RDD转换为DataFrame
 * 这里演示的是第1种
 */
public class _01SparkRDD2DataFrame {
    public static void main(String[] args) {
        Logger.getLogger("org.apache.spark").setLevel(Level.OFF);
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName(_01SparkRDD2DataFrame.class.getSimpleName())
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .registerKryoClasses(new Class[]{Person.class});
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(jsc);
        List<Person> persons = Arrays.asList(
                new Person(1, "孙人才", 25, 179),
                new Person(2, "刘银鹏", 22, 176),
                new Person(3, "郭少波", 27, 178),
                new Person(1, "齐彦鹏", 24, 175)
        );

        DataFrame df = sqlContext.createDataFrame(persons, Person.class);   // 构造方法有多个，使用personsRDD的方法也是可以的

        // where age > 23 and height > 176
        df.select(new Column("id"),
                  new Column("name"),
                  new Column("age"),
                  new Column("height"))
                .where(new Column("age").gt(23).and(new Column("height").lt(179)))
                .show();

        df.registerTempTable("person");

        sqlContext.sql("select * from person where age > 23 and height < 179").show();

        jsc.close();

    }
}
