package com.kong.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by kong on 2016/3/26 0026.
 */
public class SparkSQLJDBC2MySQL {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("jdbc");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        //通过format指定来源于jdbc
        DataFrameReader reader = sqlContext.read().format("jdbc");
        reader.option("url","jdbc:mysql://localhost:3306/shop");
        reader.option("dbtable","admin");
        reader.option("driver","com.mysql.jdbc.Driver");
        reader.option("user","root");
        reader.option("password","307453787");

        //如果数据库中数据规模特别大，此时采用传统db处理的话，需要分批次处理
        //由于SparkSQL加载db中的数据需要时间，一般会在他们之间加上缓冲层次，例如Redis
        DataFrame admin = reader.load();

        reader.option("dbtable", "user");
        DataFrame user = reader.load();

        JavaPairRDD<String, Tuple2<Integer, Integer>> join = admin.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<String, Integer>(String.valueOf(row.getAs("name")), Integer.parseInt(row.getAs("score").toString()));
            }
        }).join(user.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<String, Integer>(String.valueOf(row.getAs("name")), Integer.parseInt(row.getAs("age").toString()));
            }
        }));

        JavaRDD<Row> map = join.map(new Function<Tuple2<String, Tuple2<Integer, Integer>>, Row>() {
            @Override
            public Row call(Tuple2<String, Tuple2<Integer, Integer>> tuple) throws Exception {
                return RowFactory.create(tuple._1(), tuple._2()._1(), tuple._2()._2());
            }
        });

        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, false));
        structFields.add(DataTypes.createStructField("score", DataTypes.StringType, false));
        structFields.add(DataTypes.createStructField("age", DataTypes.StringType, false));

        StructType structType = DataTypes.createStructType(structFields);

        DataFrame dataFrame = sqlContext.createDataFrame(map, structType);

        dataFrame.show();

        //当DataFrame要把处理后的数据写入数据库必须确保授权当前操作Spark SQL的用户
        dataFrame.javaRDD().foreachPartition(new VoidFunction<Iterator<Row>>() {
            @Override
            public void call(Iterator<Row> rowIterator) throws Exception {

            }
        });

    }
}
