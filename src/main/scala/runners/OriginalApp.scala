package runners

import com.typesafe.config.ConfigFactory
import daos.db.DbDao
import daos.memory.FileDao
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, struct}
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

    val dao = new DbDao(spark)
    val fileDao = new FileDao(spark, "none", "src/main/resources/annotatedArticles")
    val articles = dao.getNewsArticles(Some(200))

    val articlesWithText = Conversion.prepareArticlesForPipeline(articles)

    val posPipeline = new PosPipeline(spark)
    //val annotations = posPipeline.runPipeline(articlesWithText)
    //val annotations = posPipeline.runPipeline(articles)
    val annotations = posPipeline.annotate(articlesWithText, "src/main/resources/models/posPipelineModel")
    //annotations.select("lemma").show(false)

    //annotations.printSchema()



    val prepared = Conversion.prepareArticlesForSaving(annotations)
    prepared.printSchema()
    val adjustedSchema = prepared.withColumn("crawl_time",
      struct(col("crawl_time").cast("long").alias("$date")))
      .withColumnRenamed("pos_percentage", "posPercentage")
    adjustedSchema.printSchema()

    fileDao.writeAnnotatedArticles(adjustedSchema)



    //annotations.select("text","pos", "lemma").show(true)
    //annotations.printSchema()


    //val annotatedArticles = Conversion.prepareArticlesForSaving(annotations, spark)
    //dao.writeArticles(annotatedArticles)
  }
}
