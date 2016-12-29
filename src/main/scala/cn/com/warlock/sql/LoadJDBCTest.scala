package cn.com.warlock.sql

import java.util.Properties

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object LoadJDBCTest {
  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println("Usage: <conn-url> <table-for-read> <user> <passwd>")
      System.exit(1)
    }

    val url = args(0)
    val table = args(1)
    val user = args(2)
    val passwd = args(3)

    val conf = new SparkConf().setAppName("LoadJDBCTest")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val props = new Properties()
    props.put("user", user)
    props.put("password", passwd)

    val df = sqlContext.read.jdbc(url, table, props)

    println("Record count: " + df.count())

    df.show()

    sc.stop()
  }
}