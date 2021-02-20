package evaluation

import com.typesafe.config.ConfigFactory
import daos.memory.FileDao

import org.apache.spark.sql.SparkSession
import pipeline.pos.PosPipeline
import utils.Conversion

import scala.io.Source

object EvaluationRunner {

  //val articlesToEvaluate = ConfigFactory.load().getString("app.file_eval")
  //val testPosTags = ConfigFactory.load().getString("app.pos_tags_eval")
  //val posModel = ConfigFactory.load().getString("app.pos_tagger_model")

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .appName("POSPipelineGerman")
      .master("local[*]")
      .getOrCreate()

    val dao = new FileDao(spark, "src/main/resources/evaluation/testArticles", "src/main/resources/evaluation/testAnnotatedArticles")
    val articles = dao.getNewsArticles()
    val preparedArticles = Conversion.prepareArticlesForPipeline(articles)
    val pipeline = new PosPipeline(spark, "src/main/resources/models/pos_ud_hdt_de_2.0.8_2.4_1561232528570")
    val annotated = pipeline.annotate(preparedArticles, "src/main/resources/models/posPipelineModel")
    annotated.select("normalized")show(false)
    //val preparedAnnotated = Conversion.prepareArticlesForSaving(annotated)
    //dao.writeAnnotatedArticles(preparedAnnotated)

   /* import spark.implicits._

    val dao = new FileDao(spark, articlesToEvaluate, "none")

    val replacements = Seq((" ", " "), ("(?<=[^A-Z\\d])\\b\\.\\b", ". "))
    val articles = Conversion.prepareArticlesForPipeline(dao.getNewsArticles(None))

    val posPipeline = new PosPipeline(spark, posModel)

    val annotations = posPipeline.runPipeline(articles)
    val tokenAndPos = annotations
      .select("finished_normalized", "finished_pos")
      .map(row => row.getSeq[String](0).toList.zip(row.getSeq[String](1).toList))
      .collect()
      .toList

    println(tokenAndPos)

    val testAnnotations = annotations
      .select("finished_pos")
      .map(row => row.getSeq[String](0).toList)
      .collect()
      .toList

    //val correctPosTags = readTestFile(testPosTags).map(line => parsePosTags(line))
    val evaluator = new PipelineEvaluator
    //val accuracy = evaluator.getAccuracy(testAnnotations, correctPosTags)
    //println("accuracy: "+accuracy)
    //println(evaluator.compare(testAnnotations, correctPosTags))*/
  }

  def readTestFile(path: String): List[String] = {
    val bufferedSource = Source.fromFile(path)
    val lines = bufferedSource.getLines().toList
    bufferedSource.close()
    lines
  }
}
