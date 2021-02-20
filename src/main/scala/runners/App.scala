package runners

import com.typesafe.config.ConfigFactory
import daos.db.DbDao
import org.apache.spark.sql.SparkSession
import pipeline.pos.PosPipeline
import training.pos.PosTrainer
import utils.Conversion

object App {
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
    //val replacements = Seq((" ", " "), ("(?<=[^A-Z\\d])\\b\\.\\b", ". "))
    val articlesWithText = Conversion.prepareArticlesForPipeline(dao.getNewsArticles(None))
    val posModel = ConfigFactory.load().getString("app.pos_tagger_model")
    val pipe = new PosPipeline(spark, posModel)
    pipe.train(articlesWithText, Some("src/main/resources/models/posPipelineModel"))
    //val annotatedArticles = pipe.runPipeline(articlesWithText, Some(path))
    //val finalDf = Conversion.prepareArticlesForSaving(annotatedArticles)
    //dao.writeAnnotatedArticles(finalDf)



    /*val trainer = new PosTrainer(spark, None, dao)
    trainer.startTraining(Some(path))
    trainer.results(None, path, true)*/
  }
}
