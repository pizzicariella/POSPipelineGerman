package daos.db

import daos.DAO
import org.mongodb.scala.{Document, MongoClient, MongoCollection}
import daos.db.DbUtils._
import model.{AnalysedArticle, NewsArticle, Strings}
import utils.json.{JsonComposer, JsonParser}

class DbDao(val userName: String,
            val pw: String,
            val serverAddress: String,
            val port: String,
            val db: String) extends DAO{

  val mongoClient = createClient(userName, pw, serverAddress, port, db)

  private def createClient(userName: String,
                           pw: String,
                           serverAddress: String,
                           port: String,
                           db: String): MongoClient = {
    MongoClient(Strings.mongoString(serverAddress, userName, pw, port, db))
  }

  private def getCollectionFromDb(dbName: String,
                                  collectionName: String,
                                  mongoClient: MongoClient): MongoCollection[Document] = {
    mongoClient.getDatabase(dbName).getCollection(collectionName)
  }

  override def getNewsArticles(limit: Option[Int], collectionName: String): Seq[NewsArticle] = {
    val docs = getCollectionFromDb(db, collectionName, mongoClient).find()

    val results = limit match {
      case Some(x) => docs.limit(x).results()
      case None => docs.results()
    }

    results.map(doc => JsonParser.parseNewsArticle(doc.toJson()))
  }

  override def writeArticle(article: AnalysedArticle, collectionName: String): Unit = {
    val json = JsonComposer.composeAnalysedArticleJson(article)
    val articleDoc = Document(json)
    val collection = getCollectionFromDb(db, collectionName, mongoClient)
    collection.insertOne(articleDoc).results()
  }

  def close() = {
    mongoClient.close()
  }
}
