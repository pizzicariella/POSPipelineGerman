package data

trait DAO {
  /**
   * Gets articles from Source.
   * columns: target columns
   * limit: maximum number of articles
   * @return An Array containing full text of articles.
   */
  def getArticles(columns: Array[String], limit: Option[Int]): Seq[Map[String, Any]]

  /**
   * Writes articles to destination.
   * @param articles
   */
  def writeArticles(articles: Seq[String])
}
