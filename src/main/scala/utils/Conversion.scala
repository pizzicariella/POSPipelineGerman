package utils

import model.{NewsArticle, Strings}

object Conversion {

  //TODO how to handle non string columns?
  def getArticleWithCompleteText(articleMap: Map[String, Any],
                                 textColumns: Array[String],
                                 otherRelevantColumns: Array[String]): (Array[String], String) = {

    //TODO add special char to distinguish title intro and text?
    val text = textColumns.map(columnName =>
      articleMap.getOrElse(columnName,
        throw new NoSuchElementException(Strings.noSuchColumnString(columnName))) match {
        case s: String => s
        case _ => Strings.empty
      })
      .reduce((s1, s2) => s1 + Strings.whitespace + s2)

    (otherRelevantColumns.map(columnName =>
      articleMap.getOrElse(columnName,
        throw new NoSuchElementException(Strings.noSuchColumnString(columnName))).toString), text)
  }

  def prepareArticles(articles: Seq[NewsArticle]): Seq[(String, String, String, String)] = {
    articles.map(article => Conversion.switchArticleFormat(article))
  }

  def switchArticleFormat(article: NewsArticle): (String, String, String, String) = {
    (article.id,
      article.longUrl,
      article.crawlTime,
      article.title+Strings.whitespace+article.intro+Strings.whitespace+article.text)
  }
}
