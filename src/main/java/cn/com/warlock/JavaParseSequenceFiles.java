package cn.com.warlock;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;


public class JavaParseSequenceFiles {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: JavaParseSequenceFiles <inputFile>");
            System.exit(1);
        }

        String inputFile = args[0];
        // String inputFile =
        // "hdfs://hlg-2p238-fandongsheng:8020/tmp/example/seq1";
        // 下面配置供windows操作系统下使用
        // System.setProperty("hadoop.home.dir", "E:\\tools");

        SparkConf sparkConf = new SparkConf().setAppName("JavaParseSequenceFiles");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        JavaPairRDD<Integer, String> lines = jsc.sequenceFile(inputFile, IntWritable.class, Text.class).mapToPair(new PairFunction<Tuple2<IntWritable, Text>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<IntWritable, Text> t) throws Exception {
                Tuple2<Integer, String> tuple2 = new Tuple2<Integer, String>(t._1.get(), t._2.toString());
                return tuple2;
            }
        });

        for (Tuple2<Integer, String> tuple2 : lines.collect()) {
            System.out.println(tuple2._1 + ":" + tuple2._2());
        }

        jsc.close();
    }
}
