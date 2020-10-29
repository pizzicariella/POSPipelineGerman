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
      .getOrCreate()

    val dao = new DbDao(Array("title", "intro", "text"), Some(10))
    val articles = dao.getArticles
    println(articles.size)

    val posPipeline = new PosPipeline(sc)
    val annotations = posPipeline.runPipeline(articles)
    annotations.select("finished_token", "finished_pos").show()

    //TODO find out what results mean and how good they are
    //TODO write tests
  }
}
