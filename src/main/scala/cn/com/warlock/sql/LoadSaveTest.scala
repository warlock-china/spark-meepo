package cn.com.warlock.sql

import java.io.File

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

object LoadSaveTest {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: <input-file> <output-dir>")
      System.exit(1)
    }

    val inputFile = args(0)
    val outputDir = args(1)

    val conf = new SparkConf().setAppName("LoadSaveTest")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val df = sc.textFile(inputFile).map(_.split( """\s+""")).map(p => Weather(p(0), p(1), p(2).toInt, p(3).toInt)).toDF

    println("Source file count: " + df.count())

    // 数据的存储，parquet 格式，设置为 append
    val f = outputDir + File.separator + "weather.parquet"
    df.write.mode(SaveMode.Append).parquet(f)
    df.write.mode(SaveMode.Append).parquet(f)

    val twiceAppend = sqlContext.read.load(f)

    println("Append mode write twice: " + twiceAppend.count())

    // 数据的存储，parquet 格式，设置为 overwrite
    df.write.mode(SaveMode.Overwrite).parquet(f)
    df.write.mode(SaveMode.Overwrite).parquet(f)

    val twiceOverwrite = sqlContext.read.load(f)

    println("Overwrite mode write twice: " + twiceOverwrite.count())

    // 存储为 table, 注意，这里是通不过的，需要用 HiveContext，官网信息是有一定的误导性
    /*
    val table = "weather_test"

    df.write.mode(SaveMode.Overwrite).format("parquet").saveAsTable(table)

    val tableDF = sqlContext.read.table(table)

    println("Table count: " + tableDF.count())

    tableDF.show()
    */

    sc.stop()
  }
}