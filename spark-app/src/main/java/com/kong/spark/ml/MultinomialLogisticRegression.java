package com.kong.spark.ml;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import scala.Tuple2;

/**
 * 逻辑回归
 * Created by lenovo on 2016/6/14.
 */
public class MultinomialLogisticRegression {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("MultinomialLogisticRegression").setMaster("local");
        SparkContext sc = new SparkContext(conf);
        String path = "c:/kong/testData/sample_libsvm.txt";
        JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc, path).toJavaRDD();

        JavaRDD<LabeledPoint>[] split = data.randomSplit(new double[]{0.6, 0.4}, 11L);
        JavaRDD<LabeledPoint> training = split[0].cache();
        JavaRDD<LabeledPoint> test = split[1];

        //训练模型
        final LogisticRegressionModel model = new LogisticRegressionWithLBFGS().setNumClasses(10).run(training.rdd());

        //用模型预测测试数据
        JavaRDD<Tuple2<Object, Object>> predictionAndLabels = test.map(new Function<LabeledPoint, Tuple2<Object, Object>>() {
            public Tuple2<Object, Object> call(LabeledPoint labeledPoint) throws Exception {
                Double prediction = model.predict(labeledPoint.features());
                return new Tuple2<Object, Object>(prediction, labeledPoint.label());
            }
        });

        MulticlassMetrics metrics = new MulticlassMetrics(predictionAndLabels.rdd());
        double precision = metrics.precision();
        System.out.println("Precision = " + precision);

        model.save(sc, "myMLR");
        LogisticRegressionModel sameModel = LogisticRegressionModel.load(sc, "myMLR");
    }
}
