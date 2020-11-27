package evaluation

import com.typesafe.config.ConfigFactory
import daos.memory.InMemoryDao
import utils.json.JsonParser.parsePosTags
import org.apache.spark.sql.SparkSession
import pipeline.pos.PosPipeline
import utils.Utils
import model.Strings

import scala.io.Source

object EvaluationRunner {

  val articlesToEvaluate = ConfigFactory.load().getString(Strings.configEvalFile)
  val testPosTags = ConfigFactory.load().getString(Strings.configEvalPosTags)

  def main(args: Array[String]): Unit = {
    val sc: SparkSession = SparkSession
      .builder()
      .appName(Strings.sparkParamsAppName)
      .master(Strings.sparkParamsLocal)
      .getOrCreate()

    import sc.implicits._

    val dao = new InMemoryDao()

    val articles = dao.getNewsArticles(None, articlesToEvaluate).map(article => Utils.switchArticleFormat(article))

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
