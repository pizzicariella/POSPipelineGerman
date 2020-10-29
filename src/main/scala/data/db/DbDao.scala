package data.db

import com.typesafe.config.ConfigFactory
import data.DAO
import org.mongodb.scala.{Document, MongoClient, MongoCollection}
import data.db.DbUtils._
import json.JsonParser.parseDocumentText

class DbDao(columns: Array[String], limit: Option[Int]) extends DAO{

  val userName = ConfigFactory.load().getString("app.user")
  val pw = ConfigFactory.load().getString("app.pw")
  val serverAddress = ConfigFactory.load().getString("app.server")
  val port = ConfigFactory.load().getString("app.port")
  val db = ConfigFactory.load().getString("app.db")
  val mongoClient = createClient(userName, pw, serverAddress, port, db)
  val collectionName = ConfigFactory.load().getString("app.collection")

  override def getArticles: Array[String] = {
    val docs = getCollectionFromDb(db, collectionName, mongoClient)
      .find()

     val results = limit match {
      case Some(x) => docs.limit(x).results()
      case None => docs.results()
    }

    results.map(doc => parseDocumentText(doc.toJson(), columns)
          .toArray
          .map(entry => entry._2)
          .reduce(_+_))
          .toArray
  }

  def createClient(userName: String,
                   pw: String,
                   serverAddress: String,
                   port: String,
                   db: String): MongoClient = {
    MongoClient("mongodb://"+userName+":"+pw+"@"+serverAddress+":"+port+"/"+db)
  }

  def getCollectionFromDb(dbName: String,
                          collectionName: String,
                          mongoClient: MongoClient): MongoCollection[Document] = {
    mongoClient.getDatabase(dbName).getCollection(collectionName)
  }
}
