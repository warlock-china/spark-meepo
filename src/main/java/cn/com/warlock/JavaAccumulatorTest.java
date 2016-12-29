package cn.com.warlock;

import java.util.Arrays;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


public class JavaAccumulatorTest {
    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("JavaAccumulatorTest");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        // 设置 "累加器"
        Accumulator<Integer> accum = jsc.accumulator(0);

        JavaRDD<Integer> dataSet = jsc.parallelize(Arrays.asList(1, 2, 3, 4));
        for (Integer data : dataSet.collect()) {
            accum.add(data);
        }

        System.out.println("Accumulator value is :" + accum.value());

        jsc.close();
    }
}
