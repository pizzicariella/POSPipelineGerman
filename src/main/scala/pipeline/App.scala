package pipeline

import com.typesafe.config.ConfigFactory
import data.db.DbDao
import org.apache.spark.sql.SparkSession
import pipeline.pos.PosPipeline

object App {
  def main(args: Array[String]): Unit = {

    val userName = ConfigFactory.load().getString("app.user")
    val pw = ConfigFactory.load().getString("app.pw")
    val serverAddress = ConfigFactory.load().getString("app.server")
    val port = ConfigFactory.load().getString("app.port")
    val db = ConfigFactory.load().getString("app.db")
    val collectionName = ConfigFactory.load().getString("app.collection")

    val sc: SparkSession = SparkSession
      .builder()
      .appName("SparkNLPPlayground")
      .master("local[*]")
      .config("spark.executor.memory", "12g")
      .config("spark.driver.memory", "12g")
      .getOrCreate()

    val dao = new DbDao(userName, pw, serverAddress, port, db, collectionName)
    val articles = dao.getArticles(Array("title", "intro", "text"), Some(20))
    println(articles.size)

    val posPipeline = new PosPipeline(sc)
    val annotations = posPipeline.runPipeline(articles)
    annotations.select("finished_token", "finished_lemma", "finished_pos").show(truncate = false)

  }
}
