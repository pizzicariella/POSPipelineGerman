package data.db

import data.DAO
import org.mongodb.scala.{Document, MongoClient, MongoCollection}
import data.db.DbUtils._
import utils.json.JsonParser.parseRelevantAttributes

class DbDao(val userName: String,
            val pw: String,
            val serverAddress: String,
            val port: String,
            val db: String,
            val collectionName: String) extends DAO{

  val mongoClient = createClient(userName, pw, serverAddress, port, db)

  override def getArticles(columns: Array[String], limit: Option[Int]): Seq[Map[String, Any]] = {
    val docs = getCollectionFromDb(db, collectionName, mongoClient)
      .find()

    val results = limit match {
      case Some(x) => docs.limit(x).results()
      case None => docs.results()
    }

    results.map(doc => parseRelevantAttributes(doc.toJson(), columns))
  }

  private def createClient(userName: String,
                   pw: String,
                   serverAddress: String,
                   port: String,
                   db: String): MongoClient = {
    MongoClient("mongodb://"+userName+":"+pw+"@"+serverAddress+":"+port+"/"+db)
  }

  private def getCollectionFromDb(dbName: String,
                          collectionName: String,
                          mongoClient: MongoClient): MongoCollection[Document] = {
    mongoClient.getDatabase(dbName).getCollection(collectionName)
  }

  /**
   * Writes articles to destination.
   *
   * @param articles
   */
  override def writeArticles(articles: Seq[String]): Unit = ???
}
