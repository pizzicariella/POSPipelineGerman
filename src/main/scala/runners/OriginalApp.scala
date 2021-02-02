package runners

import com.typesafe.config.ConfigFactory
import daos.db.DbDao
import org.apache.spark.sql.SparkSession
import pipeline.pos.PosPipeline
import utils.Conversion

object OriginalApp {
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
    //val targetCollectionName = ConfigFactory.load().getString(Strings.targetDbConfigCollection)

    val posModel = ConfigFactory.load().getString("app.pos_tagger_model")

    val spark: SparkSession = SparkSession
      .builder()
      .appName("POSPipelineGerman")
      .master("local[*]")
      .config("spark.mongodb.input.uri", "mongodb://"+userName+":"+pw+"@"+serverAddress+":"+port+"/"+db+"."+collectionName)
      .config("spark.mongodb.output.uri", "mongodb://"+targetUserName+":"+targetPw+"@"+targetServerAddress+":"
        +targetPort+"/"+targetDb+"."+"annotated_articles_test")
      .config("spark.executor.memory", "12g")
      .config("spark.driver.memory", "12g")
      .getOrCreate()

    //val dao = new DbDao(userName, pw, serverAddress, port, db, spark)
    val dao = new DbDao(spark)
    val articles = dao.getNewsArticles(Some(200))

    val replacements = Seq((" ", " "), ("(?<=[^A-Z\\d])\\b\\.\\b", ". "))

    val articlesWithText = Conversion.prepareArticlesForPipeline(articles, replacements)

    val posPipeline = new PosPipeline(spark, posModel)

    val annotations = posPipeline.runPipeline(articlesWithText)

    annotations.select("text","pos", "lemma").show(true)
    annotations.printSchema()


    //val annotatedArticles = Conversion.prepareArticlesForSaving(annotations, spark)
    //dao.writeArticles(annotatedArticles)
  }
}
