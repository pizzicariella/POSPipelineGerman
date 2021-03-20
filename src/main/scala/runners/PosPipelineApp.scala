package runners

import com.typesafe.config.ConfigFactory
import daos.db.DbDao
import org.apache.spark.sql.SparkSession
import pipeline.pos.PosPipeline
import utils.Conversion

object PosPipelineApp {
  def main(args: Array[String]): Unit = {

    val userName = ConfigFactory.load().getString("app.user")
    val pw = ConfigFactory.load().getString("app.pw")
    val serverAddress = ConfigFactory.load().getString("app.server")
    val port = ConfigFactory.load().getString("app.port")
    val db = ConfigFactory.load().getString("app.db")
    val collectionName = ConfigFactory.load().getString("app.collection")

    val targetUserName = ConfigFactory.load().getString("app.target_user")
    val targetPw = ConfigFactory.load().getString("app.target_pw")
    val targetServerAddress = ConfigFactory.load().getString("app.target_server")
    val targetPort = ConfigFactory.load().getString("app.target_port")
    val targetDb = ConfigFactory.load().getString("app.target_db")
    val targetCollectionName = ConfigFactory.load().getString("app.target_collection")

    val spark: SparkSession = SparkSession
      .builder()
      .appName("POSPipelineGerman")
      .config("spark.mongodb.input.uri", "mongodb://"+userName+":"+pw+"@"+serverAddress+":"+port+"/"+db+"."+collectionName)
      .config("spark.mongodb.output.uri", "mongodb://"+targetUserName+":"+targetPw+"@"+targetServerAddress+":"
        +targetPort+"/"+targetDb+"."+targetCollectionName)
      .getOrCreate()

    val dao = new DbDao(spark)
    val articles = dao.getNewsArticles()

    val articlesWithText = Conversion.prepareArticlesForPipeline(articles)

    val posPipeline = new PosPipeline(spark)
    val annotations = posPipeline.runPipeline(articlesWithText, Some("SparkNLP/resources/models/posPipelineModel"))

    val prepared = Conversion.prepareArticlesForSaving(annotations)

    dao.writeAnnotatedArticles(prepared)
  }
}
