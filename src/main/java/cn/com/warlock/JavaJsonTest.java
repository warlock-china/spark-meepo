package cn.com.warlock;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;


public class JavaJsonTest {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: JavaJsonTest <inputFile>");
            System.exit(1);
        }

        String inputFile = args[0];

        SparkConf sparkConf = new SparkConf().setAppName("JavaJsonTest");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = jsc.textFile(inputFile);

        // 将cid字段和method字段使用"_"组成key,然后统计相同key的个数
        JavaRDD<String> flatMapRdd = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String t) throws Exception {
                JSONObject jsonObject = JSON.parseObject(t);
                String key = jsonObject.get("cid") + "_" + jsonObject.get("method");
                return Arrays.asList(key);
            }
        });

        JavaPairRDD<String, Integer> javaPairRDD = flatMapRdd.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> counts = javaPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });

        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?, ?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }

        jsc.close();
    }
}
