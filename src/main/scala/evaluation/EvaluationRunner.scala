package evaluation

import com.typesafe.config.ConfigFactory
import daos.memory.FileDao
import org.apache.spark.sql.SparkSession
import pipeline.pos.PosPipeline
import utils.Conversion

object EvaluationRunner {

  def main(args: Array[String]): Unit = {

    val pathTestArticles = ConfigFactory.load().getString("app.evaluation_test_articles")
    val pathGoldStandard = ConfigFactory.load().getString("app.evaluation_gold_standard")

    val spark: SparkSession = SparkSession
      .builder()
      .appName("POSPipelineGerman")
      .master("local[*]")
      .getOrCreate()

    val dao = new FileDao(spark, pathTestArticles, "none")
    val testArticles = dao.getNewsArticles()
    val goldStandard = spark.read.json(pathGoldStandard)
    val pipeline = new PosPipeline(spark, "src/main/resources/models/pos_ud_hdt_de_2.0.8_2.4_1561232528570")
    val preparedArticles = Conversion.prepareArticlesForPipeline(testArticles)
    val annotated = pipeline.annotate(preparedArticles, "src/main/resources/models/posPipelineModel")
    val preparedAnnotated = Conversion.prepareArticlesForSaving(annotated)
    val evaluator = new PosPipelineEvaluator(spark)
    val modelAccuracyDf = evaluator.evaluateModel(preparedAnnotated, goldStandard)
    modelAccuracyDf.foreach(row => "article: " + println(row.getStruct(0).getString(0) +
      " - accuracy POS-Tags: " + row.getDouble(1) + " - accuracy Lemmas: " + row.getDouble(2)))
  }
}
