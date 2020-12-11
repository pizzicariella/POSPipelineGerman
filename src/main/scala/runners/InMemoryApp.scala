package runners

import com.typesafe.config.ConfigFactory
import daos.memory.InMemoryDao
import meta.ExtraInformation
import model.{AnnotatedArticle, PosAnnotation, PosPercentage, Strings}
import org.apache.spark.sql.catalyst.expressions.{CurrentRow, NamePlaceholder}
import org.apache.spark.sql.functions.{col, concat, explode, expr, row_number, struct, translate}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}
import pipeline.pos.PosPipeline
import utils.Conversion

import scala.util.Try

object InMemoryApp {
  def main(args: Array[String]): Unit = {

    val articleFile = ConfigFactory.load().getString("app.inmemoryfile_test")
    val targetFile = ConfigFactory.load().getString("app.target_inmemoryfile")
    val posModel = ConfigFactory.load().getString(Strings.configPosModel)

    val spark: SparkSession = SparkSession
      .builder()
      .appName(Strings.sparkParamsAppName)
      .master(Strings.sparkParamsLocal)
      .config(Strings.sparkConigExecuterMemory, Strings.sparkParamsMemory)
      .config(Strings.sparkConfigDriverMemory, Strings.sparkParamsMemory)
      .getOrCreate()

    import spark.implicits._

    val dao = new InMemoryDao(spark)
    val articles = dao.getNewsArticles(Some(200), articleFile)
    val replacements = Seq((Strings.replacePatternSpecialWhitespaces, Strings.replacementWhitespaces),
      (Strings.replacePatternMissingWhitespaces, Strings.replacementMissingWhitespaces))
    val articlesWithText = Conversion.prepareArticles(articles, replacements)

    val posPipeline = new PosPipeline(spark, posModel)

    val annotations = posPipeline.runPipeline(articlesWithText)

    val metaTextPosDf = annotations.select(Strings.columnId,
      Strings.columnLongUrl,
      Strings.columnCrawlTime,
      Strings.columnText,
      Strings.columnPos)

    val dropedNested = metaTextPosDf.withColumn("pos",
      expr("transform(pos, x -> struct(x.begin as begin, x.end as end, x.result as result))"))

    val annotatedArticles = dropedNested
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
          row.getStruct(2).getStruct(0).getString(0),
          row.getString(3),
          posList,
          percentages
        )
      }
      ).toDF()

    dao.writeArticles(annotatedArticles, targetFile)
  }
}
