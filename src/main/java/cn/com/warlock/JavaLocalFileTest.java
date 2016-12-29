package cn.com.warlock;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class JavaLocalFileTest {
    public static void main(String[] args) {
    	args = new String[2];
    	args[0] = "E:\\test.txt";
        if (args.length < 1) {
            System.err.println("Usage: JavaHdfsFileTest <file>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("JavaLocalFileTest");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = jsc.textFile("file://" + args[0]);

        for (String line : lines.collect()) {
            System.out.println(line);
        }

        jsc.close();
    }
}
