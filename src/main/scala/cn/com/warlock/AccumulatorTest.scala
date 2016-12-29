package cn.com.warlock

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object AccumulatorTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("AccumulatorTest")
    val ctx = new SparkContext(sparkConf)

    val accum = ctx.accumulator(0, "My Accumulator")

    ctx.parallelize(1 to 1000000, 10).foreach(i => {
      val r = Random.nextInt(10000)
      if (5000 < r && r <= 5050) {
        accum += 1
      }
    })

    println(s"accum: ${accum.value}, ")
  }
}