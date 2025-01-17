package runners

import com.typesafe.config.ConfigFactory
import daos.db.DbDao
import org.apache.spark.sql.SparkSession
import pipeline.pos.PosPipeline
import utils.Conversion

object TrainingOnlyApp {
  def main(args: Array[String]): Unit = {
    val path = ConfigFactory.load().getString("app.pipeline_model")

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
      .master("local[*]")
      .config("spark.mongodb.input.uri", "mongodb://"+userName+":"+pw+"@"+serverAddress+":"+port+"/"+db+"."+collectionName)
      .config("spark.mongodb.output.uri", "mongodb://"+targetUserName+":"+targetPw+"@"+targetServerAddress+":"
        +targetPort+"/"+targetDb+"."+targetCollectionName)
      .config("spark.executor.memory", "15g")
      .config("spark.driver.memory", "15g")
      .config("spark.network.timeout", "600s")
      .getOrCreate()

    val dao = new DbDao(spark)
    val articlesWithText = Conversion.prepareArticlesForPipeline(dao.getNewsArticles(None))
    val pipe = new PosPipeline(spark)
    pipe.train(articlesWithText, Some("src/main/resources/models/posPipelineModel"))
  }
}
