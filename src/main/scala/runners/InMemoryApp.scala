package runners

import com.typesafe.config.ConfigFactory
import daos.memory.InMemoryDao
import model.{AnalysedArticle, PosAnnotation, Strings}
import org.apache.spark.sql.{Row, SparkSession}
import pipeline.pos.PosPipeline
import utils.Conversion

object InMemoryApp {
  def main(args: Array[String]): Unit = {

    val articleFile = ConfigFactory.load().getString("app.inmemoryfile_test")
    val targetFile = ConfigFactory.load().getString("app.target_inmemoryfile")
    val posModel = ConfigFactory.load().getString(Strings.configPosModel)

    val sc: SparkSession = SparkSession
      .builder()
      .appName(Strings.sparkParamsAppName)
      .master(Strings.sparkParamsLocal)
      .config(Strings.sparkConigExecuterMemory, Strings.sparkParamsMemory)
      .config(Strings.sparkConfigDriverMemory, Strings.sparkParamsMemory)
      .getOrCreate()

    val dao = new InMemoryDao()
    val articles = dao.getNewsArticles(Some(200), articleFile)

    val articlesWithText = Conversion.prepareArticles(articles)

    val posPipeline = new PosPipeline(sc, posModel)
    val replacements = Map(Strings.replacePatternSpecialWhitespaces -> Strings.replacementWhitespaces,
      Strings.replacePatternMissingWhitespaces -> Strings.replacementMissingWhitespaces)
    val annotations = posPipeline.runPipeline(articlesWithText, replacements)

    val metaTextPosDf = annotations.select(Strings.columnId,
      Strings.columnLongUrl,
      Strings.columnCrawlTime,
      Strings.columnText,
      Strings.columnPos)

    val analysedArticles = metaTextPosDf
      .rdd
      .map(row => AnalysedArticle(row.getString(0),
        row.getString(1),
        row.getString(2),
        row.getString(3),
        row.getSeq[Row](4)
          .map(innerRow => PosAnnotation(innerRow.getInt(1),
            innerRow.getInt(2),
            innerRow.getString(3))
          ).toList
      )
      )
      .collect()

    dao.writeArticles(analysedArticles, targetFile)
  }
}
