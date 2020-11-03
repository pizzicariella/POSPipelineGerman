package evaluation

import com.typesafe.config.ConfigFactory
import data.memory.InMemoryDao
import org.apache.spark.sql.SparkSession
import pipeline.pos.PosPipeline

object EvaluationRunner {

  val articlesToEvaluate = ConfigFactory.load().getString("app.inmemoryfile_eval")

  def main(args: Array[String]): Unit = {
    val sc: SparkSession = SparkSession
      .builder()
      .appName("SparkNLPPlayground")
      .master("local[*]")
      .getOrCreate()

    val dao = new InMemoryDao(articlesToEvaluate)
    val articles = dao.getArticles(Array("title", "intro", "text"), None)
    println(articles)

    val posPipeline = new PosPipeline(sc)
    val annotations = posPipeline.runPipeline(articles)
    //annotations.select("finished_token", "finished_lemma", "finished_pos").show(truncate = false)
  }
}
