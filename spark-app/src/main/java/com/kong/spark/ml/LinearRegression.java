package com.kong.spark.ml;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;
import scala.Tuple2;

/**
 * 线性回归
 * Created by lenovo on 2016/6/14.
 */
public class LinearRegression {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("LinearRegression").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String path = "c:/kong/testData/lpsa.data";
        JavaRDD<String> data = sc.textFile(path);
        final JavaRDD<LabeledPoint> parsedData = data.map(new Function<String, LabeledPoint>() {
            public LabeledPoint call(String s) throws Exception {
                String[] parts = s.split(",");
                String[] features = parts[1].split(" ");
                double[] v = new double[features.length];
                for (int i = 0; i < features.length - 1; i++)
                    v[i] = Double.parseDouble(features[i]);
                return new LabeledPoint(Double.parseDouble(parts[0]), Vectors.dense(v));
            }
        });

        parsedData.cache();

        int numIterations = 100;
        double stepSize = 0.00000001;
        final LinearRegressionModel model = new LinearRegressionWithSGD().train(JavaRDD.toRDD(parsedData), numIterations, stepSize);

        JavaRDD<Tuple2<Object, Object>> valuesAndPreds = parsedData.map(new Function<LabeledPoint,  Tuple2<Object, Object>>() {
            public Tuple2<Object, Object> call(LabeledPoint labeledPoint) throws Exception {
                double prediction = model.predict(labeledPoint.features());
                return new Tuple2<Object, Object>(prediction, labeledPoint.label());
            }
        });

        double MSE = new JavaDoubleRDD(valuesAndPreds.map(new Function<Tuple2<Object, Object>, Object>() {

            public Object call(Tuple2<Object, Object> objectObjectTuple2) throws Exception {
                double o1 = (Double) objectObjectTuple2._1;
                double o2 = (Double) objectObjectTuple2._2;
                return Math.pow(o1 - o2, 2.0);
            }
        }).rdd()).mean();

        System.out.println("training Mean Squared Error = " + MSE);

        model.save(sc.sc(), "myModel");
        LinearRegressionModel sameModel = LinearRegressionModel.load(sc.sc(), "myModel");
    }
}
