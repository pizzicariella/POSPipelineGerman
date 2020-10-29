package data

trait DAO {
  /**
   * Gets articles from Source.
   * @return An Array containing full text of articles.
   */
  def getArticles: Seq[String]
}
