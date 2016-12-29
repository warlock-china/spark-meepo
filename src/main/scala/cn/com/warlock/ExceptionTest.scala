package cn.com.warlock

import org.apache.spark.{SparkConf, SparkContext}

object ExceptionTest {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: throw-exception, driver-mb, executor-mb")
      System.exit(1)
    }

    val throwException = args(0) toBoolean
    val driverMb = args(1) toInt
    val executorMb = args(2) toInt

    val sparkConf = new SparkConf().setAppName("ExceptionTest")
    val sc = new SparkContext(sparkConf)

    if (throwException) {
      throw new Exception("throw exception.")
    } else {
      // 让 driver 内存溢出
      val data = new Array[String](driverMb * 1024 * 1024)

      val v = sc.parallelize(1 to 10000, 100).map(x => {
        Thread.sleep(50)
        // 让 executor 内存溢出
        val data = new Array[String](executorMb * 1024 * 1024)

        x + 1
      }).reduce(_ + _)

      println(v)

      sc.stop()
    }
  }
}