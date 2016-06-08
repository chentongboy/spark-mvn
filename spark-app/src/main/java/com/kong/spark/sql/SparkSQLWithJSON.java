package com.kong.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by kong on 2016/3/25 0025.
 */
public class SparkSQLWithJSON {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("RDD2DataFrame2").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sql = new SQLContext(sc);

        DataFrame df = sql.read().format("json").load("D://person.json");
        df.registerTempTable("person");
        df.javaRDD().collect();

        List<String> peopleList = new ArrayList<String>();

        String sqlText = "select name,age from people where name in (";
        for (int i = 0; i < peopleList.size(); i++) {
            sqlText += "'" + peopleList.get(i) + "'";
            if (i < peopleList.size() - 1) {
                sqlText += ",";
            }
        }
        sqlText += ")";
        DataFrame peopleDF = sql.sql(sqlText);
        JavaPairRDD<String, Tuple2<Integer, Integer>> join = peopleDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<String, Integer>(String.valueOf(row.getAs("name")), Integer.parseInt(row.getAs("score").toString()));
            }
        }).join(peopleDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<String, Integer>(String.valueOf(row.getAs("name")), Integer.parseInt(row.getAs("age").toString()));
            }
        }));

        JavaRDD<Row> map = join.map(new Function<Tuple2<String, Tuple2<Integer, Integer>>, Row>() {
            public Row call(Tuple2<String, Tuple2<Integer, Integer>> tuple) throws Exception {
                return RowFactory.create(tuple._1(), tuple._2()._1(), tuple._2()._2());
            }
        });

        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, false));
        structFields.add(DataTypes.createStructField("score", DataTypes.StringType, false));
        structFields.add(DataTypes.createStructField("age", DataTypes.StringType, false));

        StructType structType = DataTypes.createStructType(structFields);



    }
}
