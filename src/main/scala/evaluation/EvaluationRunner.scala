package evaluation

import com.typesafe.config.ConfigFactory
import data.memory.InMemoryDao
import json.JsonParser.parsePosTags
import org.apache.spark.sql.SparkSession
import pipeline.pos.PosPipeline

import scala.collection.mutable
import scala.io.Source

object EvaluationRunner {

  val articlesToEvaluate = ConfigFactory.load().getString("app.inmemoryfile_eval")
  val testPosTags = ConfigFactory.load().getString("app.pos_tags_eval")

  def main(args: Array[String]): Unit = {
    val sc: SparkSession = SparkSession
      .builder()
      .appName("SparkNLPPlayground")
      .master("local[*]")
      .getOrCreate()

    import sc.implicits._

    val dao = new InMemoryDao(articlesToEvaluate)
    val articles = dao.getArticles(Array("title", "intro", "text"), None)

    val posPipeline = new PosPipeline(sc)
    val annotations = posPipeline.runPipeline(articles)
    val annoList = annotations
      .select("finished_pos")
      //TODO maybe the solution is, to not work with df for this small case?
      //.map(row => row(0).asInstanceOf[mutable.ArrayBuffer[String]])
      .collect()
      .toList

    println(annoList)

    val posTags = readTestFile(testPosTags).map(line => parsePosTags(line))
    val evaluator = new AnnotatorEvaluator
    //val accuracy = evaluator.getAccuracy(annoList, posTags)
    //println(accuracy)
  }

  def readTestFile(path: String): List[String] = {
    val bufferedSource = Source.fromFile(path)
    val lines = bufferedSource.getLines().toList
    bufferedSource.close()
    lines
  }
}
