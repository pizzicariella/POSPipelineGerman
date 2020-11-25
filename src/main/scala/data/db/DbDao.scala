package data.db

import data.DAO
import org.mongodb.scala.{Completed, Document, MongoClient, MongoCollection, Observer}
import data.db.DbUtils._
import utils.json.JsonParser.parseRelevantAttributes

//TODO refactor. dbdao should be usable for one db only or for multiple dbs but clarify.
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
   *
   * @param articleJson The jsonString that should be written to db
   * @param destination A String Array containing server address, port number, user name, password, database name
   *                    and collection name in this order. Should be of size 6.
   */
  override def writeArticle(articleJson: String, destination: Array[String]): Unit = {
    if (destination.size != 6) {throw new IllegalArgumentException("Array size should be 6. " +
      "Read documentation for more information.")}
    //TODO refactor class
    mongoClient.close()
    val articleDoc = Document(articleJson)
    val mongoClientWrite = createClient(
      destination(2),
      destination(3),
      destination(0),
      destination(1),
      destination(4)
    )
    val collection = getCollectionFromDb(
      destination(4),
      destination(5),
      mongoClientWrite)
    collection.insertOne(articleDoc).results()
  }
}
