package cn.com.warlock.practice.sql

import com.mongodb.casbah.MongoClient
import com.mongodb.casbah.commons.MongoDBObject
import org.apache.log4j.Logger

class MongoDBConnection(host: String, port: Int, db: String, collection: String) {
  val logger = Logger.getLogger(getClass.getName)

  def this() = {
    this(Params.mongodbHost, Params.mongodbPort, Params.mongodbDB, Params.mongodbCollection)
  }

  val mongoClient = MongoClient(host, port)

  val dbConn = mongoClient(db)
  val collConn = dbConn(collection)
}

object MongoDBConnection {
  @volatile private var instance: MongoDBConnection = null

  def getInstance: MongoDBConnection = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = new MongoDBConnection
        }
      }
    }

    instance
  }
}