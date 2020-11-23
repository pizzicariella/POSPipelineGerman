package pipeline

import com.typesafe.config.ConfigFactory
import data.db.DbDao
import org.apache.spark.sql.{Row, SparkSession}
import pipeline.pos.PosPipeline

import scala.collection.mutable

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
    val annotations = posPipeline.runPipeline(articles, Some(" "), Some(" "))
    //annotations.select("finished_token", "finished_normalized", "finished_pos").show(truncate = false)
    val posTextDf = annotations.select("text", "pos")
    val textWithPosAnnosList = posTextDf
      .rdd
      .map(row => (row.getString(0),
                   row.getSeq[Row](1)
                      .map(innerRow => (innerRow.getInt(1),
                                        innerRow.getInt(2),
                                        innerRow.getString(3))
                      )
                   )
      )
      .collect()
      .toList
    println(textWithPosAnnosList)
  }
}
