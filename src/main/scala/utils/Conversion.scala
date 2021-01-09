package utils

import meta.ExtraInformation
import model.{AnnotatedArticle, PosAnnotation, PosPercentage, Strings}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, concat, expr, lit, regexp_replace}

object Conversion {

  def prepareArticlesForPipeline(articles: DataFrame, replacements: Seq[(String, String)]): DataFrame = {
    val dfWithTextColumn = createNewTextColumn(articles)
    val replaced = replace(dfWithTextColumn, replacements)
    removeEmptyTextStrings(replaced)
  }

  def prepareArticlesForSaving(articles: DataFrame, spark: SparkSession): DataFrame = {
    val selected = articles.select(Strings.columnId,
      Strings.columnLongUrl,
      Strings.columnCrawlTime,
      Strings.columnText,
      Strings.columnPos)

    val dropedNested = dropNestedColumns(selected)
    createDfWithObjects(dropedNested, spark)
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

  private def createDfWithObjects(df: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._
    df
      .rdd
      .map(row => {
        val posList = row.getSeq[Row](4)
          .map(innerRow => PosAnnotation(innerRow.getInt(0),
            innerRow.getInt(1),
            innerRow.getString(2))
          ).toList

        val percentages = ExtraInformation.getPosPercentage(posList)
          .map(percentage => PosPercentage(percentage._1, percentage._2))

        AnnotatedArticle(row.getStruct(0).getString(0),
          row.getString(1),
          row.getTimestamp(2),
          row.getString(3),
          posList,
          percentages
        )
      }
      ).toDF()
  }

  private def dropNestedColumns(df: DataFrame): DataFrame = {
    df.withColumn("pos",
      expr("transform(pos, x -> struct(x.begin as begin, x.end as end, x.result as result))"))
  }
}
