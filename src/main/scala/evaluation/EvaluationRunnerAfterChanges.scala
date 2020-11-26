package evaluation

import com.typesafe.config.ConfigFactory
import daos.memory.InMemoryDao
import utils.json.JsonParser.parsePosTags
import org.apache.spark.sql.SparkSession
import pipeline.pos.PosPipeline
import utils.Utils

import scala.io.Source

object EvaluationRunnerAfterChanges {

  val testArticle = ConfigFactory.load().getString("app.after_eval_article")
  val testPosTags = ConfigFactory.load().getString("app.pos_tags_after_eval")

  def main(args: Array[String]): Unit = {
    val sc: SparkSession = SparkSession
      .builder()
      .appName("SparkNLPPlayground")
      .master("local[*]")
      .getOrCreate()

    import sc.implicits._

    val dao = new InMemoryDao(testArticle)
    val articleMaps = dao.getNewsArticles(Array("id", "longUrl", "crawlTime", "title", "intro", "text"), None)
    val articlesWithText =
      articleMaps.map(map =>
        Utils.getArticleWithCompleteText(map, Array("title", "intro", "text"), Array("id", "longUrl", "crawlTime")))

    val posPipeline = new PosPipeline(sc)
    val annotations = posPipeline.runPipeline(articlesWithText, Some(" "), Some(" "))
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
