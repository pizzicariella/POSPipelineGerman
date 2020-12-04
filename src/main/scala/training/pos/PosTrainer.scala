package training.pos

import com.typesafe.config.ConfigFactory
import daos.db.DbDao
import model.{AnalysedArticle, NewsArticle, PosAnnotation, Strings}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.{Row, SparkSession}
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

  val dao = new DbDao(userName, pw, serverAddress, port, db)
  val articles = dao.getNewsArticles(numArticles, collectionName)
  dao.close()
  val articlesWithText = Conversion.prepareArticles(articles)

  val posModel = ConfigFactory.load().getString(Strings.configPosModel)

  val posPipeline = new PosPipeline(spark, posModel)
  val replacements = Map(Strings.replacePatternSpecialWhitespaces -> Strings.replacementWhitespaces,
    Strings.replacePatternMissingWhitespaces -> Strings.replacementMissingWhitespaces)

  override def startTraining(path: Option[String]): PipelineModel = {

    val model = posPipeline.train(articlesWithText, replacements)

    path match {
      case None => model
      case Some(path) => {
        model.write.overwrite().save(path)
        model }
      }
    }

  override def results(articles: Option[Seq[NewsArticle]], path: String, save: Boolean): Seq[AnalysedArticle] = {

    val annotatedDf = articles match {
      case None => posPipeline.annotate(articlesWithText, replacements, path)
      case Some(articles) => posPipeline.annotate(Conversion.prepareArticles(articles), replacements, path)
    }

    val metaTextPosDf = annotatedDf.select(Strings.columnId,
      Strings.columnLongUrl,
      Strings.columnCrawlTime,
      Strings.columnText,
      Strings.columnPos)

    val analysedArticles = metaTextPosDf
      .rdd
      .map(row => AnalysedArticle(row.getString(0),
        row.getString(1),
        BigDecimal(row.getString(2)),
        row.getString(3),
        row.getSeq[Row](4)
          .map(innerRow => PosAnnotation(innerRow.getInt(1),
            innerRow.getInt(2),
            innerRow.getString(3))
          ).toList
      )
      )
      .collect()

    if(save){
      val targetUserName = ConfigFactory.load().getString(Strings.targetDbConfigUser)
      val targetPw = ConfigFactory.load().getString(Strings.targetDbConfigPw)
      val targetServerAddress = ConfigFactory.load().getString(Strings.targetDbConfigServer)
      val targetPort = ConfigFactory.load().getString(Strings.targetDbConfigPort)
      val targetDb = ConfigFactory.load().getString(Strings.targetDbConfigDb)
      val targetCollectionName = ConfigFactory.load().getString(Strings.targetDbConfigCollection)

      val targetDao = new DbDao(targetUserName, targetPw, targetServerAddress, targetPort, targetDb)
      analysedArticles.foreach(article => targetDao.writeArticle(article, targetCollectionName))
      targetDao.close()
    }

    analysedArticles

  }
}
