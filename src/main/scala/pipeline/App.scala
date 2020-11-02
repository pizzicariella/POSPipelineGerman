package pipeline

import data.db.DbDao
import org.apache.spark.sql.SparkSession
import pipeline.pos.PosPipeline

object App {
  def main(args: Array[String]): Unit = {

    val sc: SparkSession = SparkSession
      .builder()
      .appName("SparkNLPPlayground")
      .master("local[*]")
      .config("spark.executor.memory", "12g")
      .config("spark.driver.memory", "12g")
      .getOrCreate()

    val dao = new DbDao()
    val articles = dao.getArticles(Array("title", "intro", "text"), None)
    println(articles.size)

    val posPipeline = new PosPipeline(sc)
    val annotations = posPipeline.runPipeline(articles)
    annotations.limit(10).select("finished_token", "finished_pos").show()

    //TODO write tests
  }
}
