package data.memory

import com.typesafe.config.ConfigFactory
import data.DAO
import json.JsonParser.parseDocumentText

import scala.io.Source

class InMemoryDao() extends DAO{

  val pathToArticleFile = ConfigFactory.load().getString("app.inmemoryfile")

  override def getArticles(columns: Array[String], limit: Option[Int]): Seq[String] = {

    val articles = readFile(pathToArticleFile)

    val until = limit match {
      case Some(x) => x
      case None => articles.size
    }

    val sliced = articles.slice(0, until)
    sliced.map(doc => parseDocumentText(doc, columns)
      .map(entry => entry._2)
      .toSeq
      .reduce(_+_))

  }

  private def readFile(path: String): List[String] = {
    val bufferedSource = Source.fromFile(path)
    val lines = bufferedSource.getLines().toList
    bufferedSource.close()
    lines
  }
}
