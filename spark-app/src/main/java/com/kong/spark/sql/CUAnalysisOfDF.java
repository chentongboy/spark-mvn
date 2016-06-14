package com.kong.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * 使用sql对联通信令数据进行多维度分析
 * Created by lenovo on 2016/6/12.
 */
public class CUAnalysisOfDF {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("CUAnalysisOfDF").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext hiveContext = new HiveContext(jsc);
        JavaRDD<String> javaRDD = jsc.textFile(args[0]);

        //在RDD的基础上创建类型为Row的RDD
        JavaRDD<Row> cuRDD = javaRDD.map(new Function<String, Row>() {
            public Row call(String line) throws Exception {
                String[] splited = line.split(",");
                return RowFactory.create(Long.valueOf(splited[0]), Long.valueOf(splited[1]),Long.valueOf(splited[2]),Long.valueOf(splited[3]),Double.valueOf(splited[4]),Double.valueOf(splited[5]),Integer.valueOf(splited[6]));
            }
        });

        //动态构造DataFrame的元数据，一般而言，有多少列以及每列的具体类型可能来自于JSON文件,也可以来自数据库
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("MSID",DataTypes.LongType,true));
        structFields.add(DataTypes.createStructField("START_DATE",DataTypes.LongType,true));
        structFields.add(DataTypes.createStructField("START_TIME",DataTypes.LongType,true));
        structFields.add(DataTypes.createStructField("START_LONGITUDE",DataTypes.DoubleType,true));
        structFields.add(DataTypes.createStructField("START_LATITUDE",DataTypes.DoubleType,true));
        structFields.add(DataTypes.createStructField("areacode",DataTypes.IntegerType,true));

        //构建StructType，用于最后DataFrame元数据的描述
        StructType structType = DataTypes.createStructType(structFields);
        //基于以后的MetaData以及RDD<Row>来构造DataFrame
        DataFrame dataFrame = hiveContext.createDataFrame(cuRDD, structType);
        //注册成为临时表以供后续的SQL查询操作
        dataFrame.registerTempTable("culocation");
        //进行数据的多维度分析
        DataFrame result = hiveContext.sql("select * from culocation where MSID = 10001");
        //结果提取，转换成RDD，以及结构持久化
        List<Row> collect = result.javaRDD().collect();
        for (Row row:collect){
            System.out.println(row);
        }
    }
}
