package daos.db

import daos.DAO
import org.mongodb.scala.{Document, MongoClient, MongoCollection}
import daos.db.DbUtils._
import model.{AnnotatedArticle, Strings}
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.json.JsonComposer

//TODO when mongodb is working integrate spark connector to read directly from mongo to rdd
class DbDao(val userName: String,
            val pw: String,
            val serverAddress: String,
            val port: String,
            val db: String,
            val spark: SparkSession) extends DAO{

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

  override def getNewsArticles(limit: Option[Int], collectionName: String): DataFrame = {//Seq[NewsArticle] = {
    val docs = getCollectionFromDb(db, collectionName, mongoClient).find()

    val results = limit match {
      case Some(x) => docs.limit(x).results()
      case None => docs.results()
    }

    import spark.implicits._

    val jsonlist = results.map(_.toJson())
    spark.read.json(spark.createDataset[String](jsonlist))
      .drop("short_url",
        "keywords",
        "published_time",
        "news_site",
        "image_links",
        "description",
        "authors",
        "links")
    //results.map(doc => JsonParser.parseNewsArticle(doc.toJson()))
  }

  override def writeArticle(article: AnnotatedArticle, collectionName: String): Unit = {
    val json = JsonComposer.composeAnalysedArticleJson(article)
    val articleDoc = Document(json)
    val collection = getCollectionFromDb(db, collectionName, mongoClient)
    collection.insertOne(articleDoc).results()
  }

  def close() = {
    mongoClient.close()
  }

  /**
   * Writes multiple analysed articles to destination
   *
   * @param articles
   * @param destination
   */
  override def writeArticles(articles: DataFrame, destination: String): Unit = ???
}
