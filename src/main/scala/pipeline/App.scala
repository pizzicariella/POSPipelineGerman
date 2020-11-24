package pipeline

import com.typesafe.config.ConfigFactory
import data.db.DbDao
import org.apache.spark.sql.{Row, SparkSession}
import pipeline.pos.PosPipeline
import utils.Utils

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
    val articleMaps = dao.getArticles(Array("_id", "long_url", "crawl_time", "title", "intro", "text"), Some(20))
    println(articleMaps.size)
    val articlesWithText =
      articleMaps.map(map =>
        Utils.getArticleWithCompleteText(map, Array("title", "intro", "text"), Array("_id", "long_url", "crawl_time")))

    val posPipeline = new PosPipeline(sc)
    val annotations = posPipeline.runPipeline(articlesWithText, Some(" "), Some(" "))
    //annotations.select("finished_token", "finished_normalized", "finished_pos").show(truncate = false)
    val infoTextPosDf = annotations.select("articleInfo", "text", "pos")
    //infoTextPosDf.show(false)
    val textWithPosAnnosList = infoTextPosDf
      .rdd
      .map(row => (row.getSeq[String](0),
                   row.getString(1),
                   row.getSeq[Row](2)
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
