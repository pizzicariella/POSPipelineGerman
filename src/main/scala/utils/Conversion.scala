package utils

import model.{NewsArticle, Strings}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat, lit, regexp_replace, when}

object Conversion {

 /* def prepareArticles(articles: Seq[NewsArticle]): Seq[(String, String, String, String)] = {
    articles.map(article => Conversion.switchArticleFormat(article))
  }*/

  def prepareArticles(articles: DataFrame, replacements: Seq[(String, String)]): DataFrame = {
    val dfWithTextColumn = createNewTextColumn(articles)
    replace(dfWithTextColumn, replacements)
  }

  def replace(articlesDf: DataFrame,
              replacements: Seq[(String, String)]): DataFrame = {

    @scala.annotation.tailrec
    def replaceRecursively(df: DataFrame, replacements: Seq[(String, String)]): DataFrame = replacements match {
      case Nil => df
      case x::xs => replaceRecursively(df.withColumn("text", regexp_replace(df("text"), x._1, x._2)), xs)
    }

    replaceRecursively(articlesDf, replacements)
  }

  def createNewTextColumn(articles: DataFrame): DataFrame = {
    articles.withColumn("text", concat(col("title"),
      lit(" $§$ "),
      col("intro"),
      lit(" $§$ "),
      col("text")))
      .drop("title", "intro")
  }

  def switchArticleFormat(article: NewsArticle): (String, String, String, String) = {
    (article.id,
      article.longUrl,
      article.crawlTime,
      article.title+Strings.whitespace+"$§$"+Strings.whitespace+article.intro+Strings.whitespace+"$§$ "+article.text)
  }
}
