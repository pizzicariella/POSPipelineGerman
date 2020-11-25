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
   * Writes single article to destination.
   * @param articleJson
   */
  def writeArticle(articleJson: String, destination: Array[String])
}
