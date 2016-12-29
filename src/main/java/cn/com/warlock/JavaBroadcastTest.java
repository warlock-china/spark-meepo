package cn.com.warlock;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;


public class JavaBroadcastTest {
    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("JavaBroadcastTest");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        // 设置 "广播变量"
        Broadcast<int[]> broadcastVar = jsc.broadcast(new int[] { 1, 2, 3 });

        System.out.print("Broadcast value is :");
        for (int value : broadcastVar.value()) {
            System.out.print(" " + value);
        }
        System.out.println();

        jsc.close();
    }
}
