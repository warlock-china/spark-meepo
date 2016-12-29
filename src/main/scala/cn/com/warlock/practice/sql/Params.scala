package cn.com.warlock.practice.sql

import cn.com.warlock.practice.utils.ConfigParser

object Params {
  // 相关参数配置
  val days = ConfigParser.getInstance.getInt("days")

  val sourcedataDir = ConfigParser.getInstance.getString("sourcedata.dir")
  val sourcedataPartition = ConfigParser.getInstance.getInt("sourcedata.partition")

  val hiveStatdayDelete = ConfigParser.getInstance.getBoolean("hive.statday.delete")
  val hiveWarehouseDir = ConfigParser.getInstance.getString("hive.warehousedir")

  val hiveAlluserTable = ConfigParser.getInstance.getString("hive.alluser.table")
  val hiveAlluserCreateTable = ConfigParser.getInstance.getString("hive.alluser.createtable")

  val hiveNewuserTable = ConfigParser.getInstance.getString("hive.newuser.table")
  val hiveNewuserCreateTable = ConfigParser.getInstance.getString("hive.newuser.createtable")

  val mongodbHost = ConfigParser.getInstance.getString("mongodb.host")
  val mongodbPort = ConfigParser.getInstance.getInt("mongodb.port")
  val mongodbDB = ConfigParser.getInstance.getString("mongodb.db")
  val mongodbCollection = ConfigParser.getInstance.getString("mongodb.collection")

  // 对参数进行格式化处理返回
  override def toString(): String = {
    ConfigParser.getInstance.toString()
  }
}