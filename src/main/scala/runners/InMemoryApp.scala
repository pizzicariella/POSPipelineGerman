package runners

import com.typesafe.config.ConfigFactory
import daos.memory.FileDao
import org.apache.spark.sql.SparkSession
import pipeline.pos.PosPipeline
import utils.Conversion

object InMemoryApp {
  def main(args: Array[String]): Unit = {

    //val articleFile = ConfigFactory.load().getString("app.inmemoryfile_test")
    //val targetFile = ConfigFactory.load().getString("app.target_inmemoryfile")

    val spark: SparkSession = SparkSession
      .builder()
      .appName("POSPipelineGerman")
      .master("local[*]")
      .config("spark.executor.memory", "12g")
      .config("spark.driver.memory", "12g")
      .getOrCreate()

    val dao = new FileDao(spark,
      "src/test/resources/inMemoryArticles",
      "src/main/resources/annotatedArticles")
    val articles = dao.getNewsArticles()
    val articlesWithText = Conversion.prepareArticlesForPipeline(articles)

    val posPipeline = new PosPipeline(spark)

    val annotations = posPipeline.runPipeline(articlesWithText)

    //val annotatedArticles = Conversion.prepareArticlesForSaving(annotations)
    annotations.select("normalized").show(false)

    //dao.writeAnnotatedArticles(annotatedArticles)
  }
}
