package cn.com.warlock

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.Set
import scala.io.Source

object BroadcastTest {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: <local-dict> <hadoop-file>")
      System.exit(1)
    }

    // 加载词典，并且设置为 "广播变量"
    val sources = Source.fromFile(args(0))
    val dict = Set[String]()

    for (line <- sources.getLines()) {
      dict += line
    }

    val sparkConf = new SparkConf().setAppName("BroadcastTest")
    val ctx = new SparkContext(sparkConf)
    val broadcastVar = ctx.broadcast(dict)

    // 加载 hadoop file，判断是否包含特定的词典
    val sentences = ctx.textFile(args(1)).flatMap(x => x.split("\\."))

    val rdd = sentences.filter(x => {
      val words = x.split("\\s+")
      words.exists(x => broadcastVar.value.contains(x))
    })

    rdd.collect.foreach(println)

    ctx.stop()
  }
}