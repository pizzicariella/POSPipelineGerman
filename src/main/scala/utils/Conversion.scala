package utils

import model.{NewsArticle, Strings}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, concat, lit, regexp_replace, struct, when}
import org.apache.spark.sql.types.{StructField, StructType}

import scala.util.Try

object Conversion {

 /* def prepareArticles(articles: Seq[NewsArticle]): Seq[(String, String, String, String)] = {
    articles.map(article => Conversion.switchArticleFormat(article))
  }*/

  def prepareArticles(articles: DataFrame, replacements: Seq[(String, String)]): DataFrame = {
    val dfWithTextColumn = createNewTextColumn(articles)
    val replaced = replace(dfWithTextColumn, replacements)
    removeEmptyTextStrings(replaced)
  }

  def removeEmptyTextStrings(df: DataFrame): DataFrame = df.filter("text != ''")

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

  def dropNestedColumns(df: DataFrame, sourceColumn: String, toDrop: Array[String]): DataFrame = {
    def getSourceField(source: String): Try[StructField] = {
      Try(df.schema.fields.filter(_.name == source).head)
    }

    def getType(sourceField: StructField): Try[StructType] = {
      Try(sourceField.dataType.asInstanceOf[StructType])
    }

    def genOutputCol(names: Array[String], source: String): Column = {
      struct(names.map(x => col(source).getItem(x).alias(x)): _*)
    }

    getSourceField(sourceColumn)
      .flatMap(getType)
      .map(_.fieldNames.diff(toDrop))
      .map(genOutputCol(_, sourceColumn))
      .map(df.withColumn(sourceColumn, _))
      .getOrElse(df)
  }

  def switchArticleFormat(article: NewsArticle): (String, String, String, String) = {
    (article.id,
      article.longUrl,
      article.crawlTime,
      article.title+Strings.whitespace+"$§$"+Strings.whitespace+article.intro+Strings.whitespace+"$§$ "+article.text)
  }
}
