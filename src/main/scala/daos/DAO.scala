package daos

import org.apache.spark.sql.DataFrame

trait DAO {
  /**
   * Gets articles from Source and returns them as DataFrame.
   * @param limit: maximum number of articles. Pass None (default) to get all articles from source. If source contains
   * less articles than limit, all available articles will be returned.
   * @return A DataFrame containing news articles.
   */
  def getNewsArticles(limit: Option[Int] = None): DataFrame


  /**
   * Writes multiple articles to destination.
   * @param articles: A DataFrame containing articles.
   */
  def writeAnnotatedArticles(articles: DataFrame)
}
