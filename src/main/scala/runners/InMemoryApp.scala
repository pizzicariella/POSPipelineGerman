package runners

import com.typesafe.config.ConfigFactory
import daos.memory.InMemoryDao
import org.apache.spark.sql.SparkSession
import pipeline.pos.PosPipeline
import utils.Conversion

object InMemoryApp {
  def main(args: Array[String]): Unit = {

    val articleFile = ConfigFactory.load().getString("app.inmemoryfile_test")
    val targetFile = ConfigFactory.load().getString("app.target_inmemoryfile")
    val posModel = ConfigFactory.load().getString("app.pos_tagger_model")

    val spark: SparkSession = SparkSession
      .builder()
      .appName("POSPipelineGerman")
      .master("local[*]")
      .config("spark.executor.memory", "12g")
      .config("spark.driver.memory", "12g")
      .getOrCreate()

    val dao = new InMemoryDao(spark, articleFile, targetFile)
    val articles = dao.getNewsArticles(Some(200))
    val replacements = Seq((" ", " "),
      ("(?<=[^A-Z\\d])\\b\\.\\b", ". "))
    val articlesWithText = Conversion.prepareArticlesForPipeline(articles, replacements)

    val posPipeline = new PosPipeline(spark, posModel)

    val annotations = posPipeline.runPipeline(articlesWithText)

    val annotatedArticles = Conversion.prepareArticlesForSaving(annotations)

    dao.writeAnnotatedArticles(annotatedArticles)
  }
}
