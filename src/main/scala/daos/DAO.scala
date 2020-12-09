package daos

import model.{AnnotatedArticle, NewsArticle}

trait DAO {
  /**
   * Gets articles from Source and returs a Sequence of NewsArticle objects.
   * source: source of articles (e.g. collection name, path to file...)
   * limit: maximum number of articles
   * @return A Sequence containing NewsArticle objects.
   */
  def getNewsArticles(limit: Option[Int], source: String): Seq[NewsArticle]

  /**
   * Writes single analysed article to destination.
   * @param article
   */
  def writeArticle(article: AnnotatedArticle, destination: String)

  /**
   * Writes multiple analysed articles to destination
   * @param articles
   * @param destination
   */
  def writeArticles(articles: Seq[AnnotatedArticle], destination: String)
}
