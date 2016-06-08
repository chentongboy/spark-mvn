package com.kong.spark.sql;

import com.kong.spark.bean.Person;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;

/**
 * 使用反射的方式将RDD转换成为DataFrame
 * 注意：bean需要public修饰，实现Serializable
 * 变成Row的时候按照属性的首字母排序
 * Created by kong on 2016/3/15 0015.
 */
public class RDD2DataFrameByReflection {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("RDD2DataFrameByReflection");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        JavaRDD<String> lines = sc.textFile("D://person.txt");
        JavaRDD<Person> persons = lines.map(new Function<String, Person>() {
            public Person call(String line) throws Exception {
                String[] splited = line.split(",");
                Person p = new Person();
                p.setId(Integer.valueOf(splited[0]));
                p.setName(splited[1].trim());
                p.setAge(Integer.valueOf(splited[2].trim()));
                return p;
            }
        });
        DataFrame df = sqlContext.createDataFrame(persons, Person.class);
        df.registerTempTable("personTable");
        DataFrame bigDatas = sqlContext.sql("select * from personTable where age >= 6");
        JavaRDD<Row> bigDataRDD = bigDatas.javaRDD();
        JavaRDD<Person> result = bigDataRDD.map(new Function<Row, Person>() {
            public Person call(Row row) throws Exception {
                Person p = new Person();
                p.setId(row.getInt(1));
                p.setName(row.getString(2));
                p.setAge(row.getInt(0));
                return p;
            }
        });

        List<Person> personList = result.collect();
        for (Person p : personList){
            System.out.println(p);
        }

    }
}
