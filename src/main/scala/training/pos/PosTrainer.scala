package training.pos

import com.typesafe.config.ConfigFactory
import daos.db.DbDao
import meta.ExtraInformation
import model.{AnnotatedArticle, NewsArticle, PosAnnotation, PosPercentage, Strings}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
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

  val dao = new DbDao(userName, pw, serverAddress, port, db, spark)
  val articles = dao.getNewsArticles(numArticles, collectionName)
  dao.close()
  val replacements = Seq((Strings.replacePatternSpecialWhitespaces, Strings.replacementWhitespaces),
    (Strings.replacePatternMissingWhitespaces, Strings.replacementMissingWhitespaces))
  val articlesWithText = Conversion.prepareArticles(articles, replacements)

  val posModel = ConfigFactory.load().getString(Strings.configPosModel)

  val posPipeline = new PosPipeline(spark, posModel)

  import spark.implicits._


  override def startTraining(path: Option[String]): PipelineModel = {

    val model = posPipeline.train(articlesWithText)

    path match {
      case None => model
      case Some(path) => {
        model.write.overwrite().save(path)
        model }
      }
    }

  override def results(articles: Option[DataFrame], path: String, save: Boolean): DataFrame = {

    val annotatedDf = articles match {
      case None => posPipeline.annotate(articlesWithText, path)
      case Some(articles) => posPipeline.annotate(Conversion.prepareArticles(articles, replacements), path)
    }

    val metaTextPosDf = annotatedDf.select(Strings.columnId,
      Strings.columnLongUrl,
      Strings.columnCrawlTime,
      Strings.columnText,
      Strings.columnPos)

    val dropedNested = metaTextPosDf.withColumn("pos",
      expr("transform(pos, x -> struct(x.begin as begin, x.end as end, x.result as result))"))

    val annotatedArticles = dropedNested
      .rdd
      .map(row => {
        val posList = row.getSeq[Row](4)
          .map(innerRow => PosAnnotation(innerRow.getInt(0),
            innerRow.getInt(1),
            innerRow.getString(2))
          ).toList

        val percentages = ExtraInformation.getPosPercentage(posList)
          .map(percentage => PosPercentage(percentage._1, percentage._2))

        AnnotatedArticle(row.getStruct(0).getString(0),
          row.getString(1),
          row.getStruct(2).getStruct(0).getString(0),
          row.getString(3),
          posList,
          percentages
        )
      }
      ).toDF()

    if(save){
      val targetUserName = ConfigFactory.load().getString(Strings.targetDbConfigUser)
      val targetPw = ConfigFactory.load().getString(Strings.targetDbConfigPw)
      val targetServerAddress = ConfigFactory.load().getString(Strings.targetDbConfigServer)
      val targetPort = ConfigFactory.load().getString(Strings.targetDbConfigPort)
      val targetDb = ConfigFactory.load().getString(Strings.targetDbConfigDb)
      val targetCollectionName = ConfigFactory.load().getString(Strings.targetDbConfigCollection)

      val targetDao = new DbDao(targetUserName, targetPw, targetServerAddress, targetPort, targetDb, spark)
      //annotatedArticles.foreach(article => targetDao.writeArticle(article, targetCollectionName))
      targetDao.close()
    }

    annotatedArticles

  }
}
