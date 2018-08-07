package cn.xpleaf.bigdata.spark.java.sql.p1;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class _02SparkRDD2DataFrame {
    public static void main(String[] args) {
        Logger.getLogger("org.apache.spark").setLevel(Level.OFF);
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName(_02SparkRDD2DataFrame.class.getSimpleName())
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

        Stream<Row> rowStream = persons.stream().map(new Function<Person, Row>() {
            @Override
            public Row apply(Person person) {
                return RowFactory.create(person.getId(), person.getName(), person.getAge(), person.getHeight());
            }
        });

        List<Row> rows = rowStream.collect(Collectors.toList());

        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("age", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("height", DataTypes.DoubleType, false, Metadata.empty())
        });

        DataFrame df = sqlContext.createDataFrame(rows, schema);

        df.registerTempTable("person");

        sqlContext.sql("select * from person where age > 23 and height < 179").show();

        jsc.close();

    }
}
