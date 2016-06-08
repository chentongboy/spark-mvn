package com.kong.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by kong on 2016/3/16 0016.
 */
public class RDD2DataFrameByProgrammatically {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("RDD2DataFrame2").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sql = new SQLContext(sc);
        JavaRDD<String> lines = sc.textFile("D://person.txt");
        //在RDD的基础上创建类型为Row的RDD
        JavaRDD<Row> personsRDD = lines.map(new Function<String, Row>() {
            @Override
            public Row call(String line) throws Exception {
                String[] splited = line.split(",");
                return RowFactory.create(Integer.valueOf(splited[0]), splited[1], Integer.valueOf(splited[2]));
            }
        });

        //动态构造DataFrame的元数据，一般而言，有多少列以及每列的具体类型可能来自于JSON文件,也可以来自数据库
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("id",DataTypes.IntegerType,true));
        structFields.add(DataTypes.createStructField("name",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("age",DataTypes.IntegerType,true));
        //构建StructType，用于最后DataFrame元数据的描述
        StructType structType = DataTypes.createStructType(structFields);
        //基于以后的MetaData以及RDD<Row>来构造DataFrame
        DataFrame dataFrame = sql.createDataFrame(personsRDD, structType);
        //注册成为临时表以供后续的SQL查询操作
        dataFrame.registerTempTable("person");
        //进行数据的多维度分析
        DataFrame result = sql.sql("select * from person where age < 10");
        //结果提取，转换成RDD，以及结构持久化
        List<Row> collect = result.javaRDD().collect();
        for (Row row:collect){
            System.out.println(row);
        }
    }
}
