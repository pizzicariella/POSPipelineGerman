package pipeline

import data.db.DbDao

object App {
  def main(args: Array[String]): Unit = {

    val dao = new DbDao(Array("title", "intro", "text"), None)
    val articles = dao.getArticles
    println(articles.size)

  }
}
