package utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat, expr, lit, regexp_replace, udf}


object Conversion {

  def prepareArticlesForPipeline(articles: DataFrame): DataFrame = {
    val dfWithTextColumn = createNewTextColumn(articles)
    //val replaced = replace(dfWithTextColumn, replacements)
    removeEmptyTextStrings(dfWithTextColumn)
  }

  def prepareArticlesForSaving(articles: DataFrame): DataFrame = {
    val selected = articles.select(
      "_id",
      "long_url",
      "crawl_time",
      "text",
      "pos",
      "lemma")

    val dropedNested = dropNestedColumns(selected)
    createPosPercentageColumn(dropedNested)
  }

  private def removeEmptyTextStrings(df: DataFrame): DataFrame = df.filter("text != ''")

  private def replace(articlesDf: DataFrame,
              replacements: Seq[(String, String)]): DataFrame = {

    @scala.annotation.tailrec
    def replaceRecursively(df: DataFrame, replacements: Seq[(String, String)]): DataFrame = replacements match {
      case Nil => df
      case x::xs => replaceRecursively(df.withColumn("text", regexp_replace(df("text"), x._1, x._2)), xs)
    }

    replaceRecursively(articlesDf, replacements)
  }

  private def createNewTextColumn(articles: DataFrame): DataFrame = {
    articles.withColumn("text", concat(col("title"),
      lit(" $§$ "),
      col("intro"),
      lit(" $§$ "),
      col("text")))
      .drop("title", "intro")
  }

  private def createPosPercentageColumn(df: DataFrame): DataFrame = {

    //TODO vielleicht doch mit aggregate möglich
    val getPosPercentage = (annos: Seq[String]) => {
      annos.foldLeft(Map.empty[String, Double])((map, anno) => map.updated(anno, map.getOrElse(anno, 0.0)+1.0))
        .mapValues(_/annos.size)
        .toList
    }

    val getPosPercentageUDF = udf(getPosPercentage)

    df.withColumn("pos_percentage", expr("transform(pos, x -> x.tag)"))
      .withColumn("pos_percentage", getPosPercentageUDF(col("pos_percentage")))
      .withColumn("pos_percentage", expr("transform(pos_percentage, x -> struct(x._1 as tag, x._2 as percentage))"))

  }

  private def dropNestedColumns(df: DataFrame): DataFrame = {
    df.withColumn("pos",
        expr("transform(pos, x -> struct(x.begin as begin, x.end as end, x.result as tag))"))
      .withColumn("lemma",
        expr("transform(lemma, x -> struct(x.begin as beginToken, x.end as endToken, x.result as result))"))
  }
}
