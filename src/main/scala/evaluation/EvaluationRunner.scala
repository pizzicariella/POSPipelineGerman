package evaluation

import com.typesafe.config.ConfigFactory
import daos.memory.FileDao

import org.apache.spark.sql.SparkSession
import pipeline.pos.PosPipeline
import utils.Conversion

import scala.io.Source

object EvaluationRunner {

  val articlesToEvaluate = ConfigFactory.load().getString("app.file_eval")
  val testPosTags = ConfigFactory.load().getString("app.pos_tags_eval")
  val posModel = ConfigFactory.load().getString("app.pos_tagger_model")

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("POSPipelineGerman")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val dao = new FileDao(spark, articlesToEvaluate, "none")

    val replacements = Seq((" ", " "), ("(?<=[^A-Z\\d])\\b\\.\\b", ". "))
    val articles = Conversion.prepareArticlesForPipeline(dao.getNewsArticles(None), replacements)

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
    //println(evaluator.compare(testAnnotations, correctPosTags))
  }

  def readTestFile(path: String): List[String] = {
    val bufferedSource = Source.fromFile(path)
    val lines = bufferedSource.getLines().toList
    bufferedSource.close()
    lines
  }
}
