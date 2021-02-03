package daos

import org.apache.spark.sql.DataFrame

trait DAO {
  /**
   * Gets articles from Source and returs a Sequence of NewsArticle objects.
   * source: source of articles (e.g. collection name, path to file...)
   * limit: maximum number of articles
   * @return A Sequence containing NewsArticle objects.
   */
  def getNewsArticles(limit: Option[Int]): DataFrame


  /**
   * Writes multiple analysed articles to destination
   * @param articles
   * @param destination
   */
  def writeArticles(articles: DataFrame)
}
