package training.pos

import com.typesafe.config.ConfigFactory
import daos.db.DbDao
import model.Strings
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.{DataFrame, SparkSession}
import pipeline.pos.PosPipeline
import training.Trainer
import utils.Conversion

class PosTrainer(spark: SparkSession, numArticles: Option[Int]) extends Trainer{

  val userName = ConfigFactory.load().getString(Strings.dbConfigUser)
  val pw = ConfigFactory.load().getString(Strings.dbConfigPw)
  val serverAddress = ConfigFactory.load().getString(Strings.dbConfigServer)
  val port = ConfigFactory.load().getString(Strings.dbConfigPort)
  val db = ConfigFactory.load().getString(Strings.dbConfigDb)
  val collectionName = ConfigFactory.load().getString(Strings.dbConfigCollection)

  //val dao = new DbDao(userName, pw, serverAddress, port, db, spark)
  val dao = new DbDao(spark)
  val articles = dao.getNewsArticles(numArticles, collectionName)
  dao.close()
  val replacements = Seq((Strings.replacePatternSpecialWhitespaces, Strings.replacementWhitespaces),
    (Strings.replacePatternMissingWhitespaces, Strings.replacementMissingWhitespaces))
  val articlesWithText = Conversion.prepareArticlesForPipeline(articles, replacements)

  val posModel = ConfigFactory.load().getString(Strings.configPosModel)

  val posPipeline = new PosPipeline(spark, posModel)

  override def startTraining(path: Option[String]): PipelineModel = {

    val model = posPipeline.train(articlesWithText)

    path match {
      case None => model
      case Some(path) => {
        model.write.overwrite().save(path)
        model }
      }
    }

  //TODO divide in 2 methods, 1 that uses annotate method, one that uses run method and get rid of option
  override def results(articles: Option[DataFrame], path: String, save: Boolean): DataFrame = {

    val annotatedDf = articles match {
      case None => posPipeline.annotate(articlesWithText, path)
      case Some(articles) => posPipeline.annotate(Conversion.prepareArticlesForPipeline(articles, replacements), path)
    }

    val finalDf = Conversion.prepareArticlesForSaving(annotatedDf, spark)

    if(save){
      val targetUserName = ConfigFactory.load().getString(Strings.targetDbConfigUser)
      val targetPw = ConfigFactory.load().getString(Strings.targetDbConfigPw)
      val targetServerAddress = ConfigFactory.load().getString(Strings.targetDbConfigServer)
      val targetPort = ConfigFactory.load().getString(Strings.targetDbConfigPort)
      val targetDb = ConfigFactory.load().getString(Strings.targetDbConfigDb)
      val targetCollectionName = ConfigFactory.load().getString(Strings.targetDbConfigCollection)

      //val targetDao = new DbDao(targetUserName, targetPw, targetServerAddress, targetPort, targetDb, spark)
      //finalDf.foreach(article => targetDao.writeArticle(article, targetCollectionName))
      //targetDao.close()
    }

    finalDf
  }
}
