package utils

import model.{NewsArticle, Strings}

object Conversion {

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
