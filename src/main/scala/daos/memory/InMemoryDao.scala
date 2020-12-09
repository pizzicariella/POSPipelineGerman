package daos.memory

import daos.DAO
import model.{AnnotatedArticle, NewsArticle}
import utils.FileIO
import utils.json.JsonComposer
import utils.json.JsonParser.parseNewsArticle

class InMemoryDao() extends DAO{

  override def getNewsArticles(limit: Option[Int], file: String): Seq[NewsArticle] = {

    val articles = FileIO.readJsonFile(file)

    val until = limit match {
      case Some(x) => x
      case None => articles.size
    }

    val sliced = articles.slice(0, until)
    sliced.map(doc => parseNewsArticle(doc))
  }

  override def writeArticle(article: AnnotatedArticle, destination: String): Unit = ???

  /**
   * Writes multiple analysed articles to destination
   *
   * @param articles
   * @param destination
   */
    //TODO test
  override def writeArticles(articles: Seq[AnnotatedArticle], destination: String): Unit = {
    val jsonList = articles.map(article => JsonComposer.composeAnalysedArticleJson(article))
    FileIO.writeJsonFile(destination, jsonList)
  }
}
