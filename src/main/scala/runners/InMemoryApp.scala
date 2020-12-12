package runners

import com.typesafe.config.ConfigFactory
import daos.memory.InMemoryDao
import model.Strings
import org.apache.spark.sql.SparkSession
import pipeline.pos.PosPipeline
import utils.Conversion

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

    val dao = new InMemoryDao(spark)
    val articles = dao.getNewsArticles(Some(200), articleFile)
    val replacements = Seq((Strings.replacePatternSpecialWhitespaces, Strings.replacementWhitespaces),
      (Strings.replacePatternMissingWhitespaces, Strings.replacementMissingWhitespaces))
    val articlesWithText = Conversion.prepareArticlesForPipeline(articles, replacements)

    val posPipeline = new PosPipeline(spark, posModel)

    val annotations = posPipeline.runPipeline(articlesWithText)

    val annotatedArticles = Conversion.prepareArticlesForSaving(annotations, spark)

    dao.writeArticles(annotatedArticles, targetFile)
  }
}
