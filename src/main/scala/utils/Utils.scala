package utils

import model.NewsArticle

object Utils {

  //TODO how to handle non string columns?
  def getArticleWithCompleteText(articleMap: Map[String, Any],
                                 textColumns: Array[String],
                                 otherRelevantColumns: Array[String]): (Array[String], String) = {

    //TODO add special char to distinguish title intro and text?
    val text = textColumns.map(columnName =>
      articleMap.getOrElse(columnName,
        throw new NoSuchElementException("The column " + columnName + " does not exist.")) match {
        case s: String => s
        case _ => ""
      })
      .reduce((s1, s2) => s1 + " " + s2)

    (otherRelevantColumns.map(columnName =>
      articleMap.getOrElse(columnName,
        throw new NoSuchElementException("The column " + columnName + " does not exist.")).toString), text)
  }

  def prepareArticles(articles: Seq[NewsArticle]): Seq[(String, String, String, String)] = {
    articles.map(article => Utils.switchArticleFormat(article))
  }

  def switchArticleFormat(article: NewsArticle): (String, String, String, String) = {
    (article.id, article.longUrl, article.crawlTime, article.title+" "+article.intro+" "+article.text)
  }


}
