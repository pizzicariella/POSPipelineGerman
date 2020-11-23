package evaluation

import com.typesafe.config.ConfigFactory
import data.memory.InMemoryDao
import json.JsonParser.parsePosTags
import org.apache.spark.sql.SparkSession
import pipeline.pos.PosPipeline
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
    println(articles)

    val posPipeline = new PosPipeline(sc)
    val annotations = posPipeline.runPipeline(articles, Some(" "), Some(" "))
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

    val correctPosTags = readTestFile(testPosTags).map(line => parsePosTags(line))
    val evaluator = new PipelineEvaluator
    val accuracy = evaluator.getAccuracy(testAnnotations, correctPosTags)
    println("accuracy: "+accuracy)
    println(evaluator.compare(testAnnotations, correctPosTags))
  }

  def readTestFile(path: String): List[String] = {
    val bufferedSource = Source.fromFile(path)
    val lines = bufferedSource.getLines().toList
    bufferedSource.close()
    lines
  }
}
