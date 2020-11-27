package daos.memory

import daos.DAO
import model.NewsArticle
import utils.FileReader
import utils.json.AnalysedArticle
import utils.json.JsonParser.parseNewsArticle

class InMemoryDao() extends DAO{

  override def getNewsArticles(limit: Option[Int], file: String): Seq[NewsArticle] = {

    val articles = FileReader.readJsonFile(file)

    val until = limit match {
      case Some(x) => x
      case None => articles.size
    }

    val sliced = articles.slice(0, until)
    sliced.map(doc => parseNewsArticle(doc))
  }

  override def writeArticle(article: AnalysedArticle, destination: String): Unit = ???
}
