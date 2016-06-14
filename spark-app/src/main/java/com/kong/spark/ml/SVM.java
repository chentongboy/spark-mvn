package com.kong.spark.ml;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import scala.Tuple2;

/**
 * Created by lenovo on 2016/6/14.
 */
public class SVM {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SVM").setMaster("local");
        SparkContext sc = new SparkContext(conf);
        String path = "c:/kong/testData/sample_libsvm.txt";
        JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc, path).toJavaRDD();

        //设置60%为训练数据，40%为测试数据
        JavaRDD<LabeledPoint> training = data.sample(false, 0.6, 11L);
        training.cache();
        JavaRDD<LabeledPoint> test = data.subtract(training);

        //训练模型
        int numIterations = 100;
        final SVMModel model = SVMWithSGD.train(training.rdd(),numIterations);

        model.clearThreshold();

        //模型计算预测数据
        JavaRDD<Tuple2<Object, Object>> scoreAndLabels = test.map(new Function<LabeledPoint, Tuple2<Object, Object>>() {
            public Tuple2<Object, Object> call(LabeledPoint labeledPoint) throws Exception {
                double score = model.predict(labeledPoint.features());
                return new Tuple2<Object, Object>(score, labeledPoint.label());
            }
        });

        BinaryClassificationMetrics metrics = new BinaryClassificationMetrics(JavaRDD.toRDD(scoreAndLabels));
        double auROC = metrics.areaUnderROC();

        System.out.println("Area under ROC = " + auROC);

        //保存训练模型
        model.save(sc,"myModelPath");

        //读取训练模型
        SVMModel svmModel = SVMModel.load(sc, "myModelPath");
    }
}
