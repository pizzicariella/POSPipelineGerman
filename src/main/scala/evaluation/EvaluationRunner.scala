package evaluation

import com.typesafe.config.ConfigFactory
import daos.memory.FileDao
import org.apache.spark.sql.SparkSession

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
    val evaluator = new PosPipelineEvaluator(spark)
    val modelAccuracyDf = evaluator.evaluateModel(testArticles, goldStandard, "src/main/resources/models/posPipelineModel")
    modelAccuracyDf.foreach(row => "article: " + println(row.getStruct(0).getString(0) +
      " - accuracy POS-Tags: " + row.getDouble(1) + " - accuracy Lemmas: " + row.getDouble(2)))

  }
}
