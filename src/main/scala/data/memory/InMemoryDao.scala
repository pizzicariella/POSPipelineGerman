package data.memory

import com.typesafe.config.ConfigFactory
import data.DAO
import utils.json.JsonParser.parseRelevantAttributes

import scala.io.Source

class InMemoryDao(val resourceFile: String) extends DAO{

  //val pathToArticleFile = ConfigFactory.load().getString("app.inmemoryfile")

  override def getArticles(columns: Array[String], limit: Option[Int]): Seq[Map[String, Any]] = {

    val articles = readFile(resourceFile)

    val until = limit match {
      case Some(x) => x
      case None => articles.size
    }

    val sliced = articles.slice(0, until)
    sliced.map(doc => parseRelevantAttributes(doc, columns))
  }

  private def readFile(path: String): List[String] = {
    val bufferedSource = Source.fromFile(path)
    val lines = bufferedSource.getLines().toList
    bufferedSource.close()
    lines
  }

  /**
   * Writes articles to destination.
   *
   * @param articles
   */
  override def writeArticles(articles: Seq[String]): Unit = ???
}
