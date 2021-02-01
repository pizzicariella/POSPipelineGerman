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

  val dao = new DbDao(spark)
  val articles = dao.getNewsArticles(numArticles)
  val replacements = Seq((Strings.replacePatternSpecialWhitespaces, Strings.replacementWhitespaces),
    (Strings.replacePatternMissingWhitespaces, Strings.replacementMissingWhitespaces))
  val articlesWithText = Conversion.prepareArticlesForPipeline(articles, replacements)

  val posModel = ConfigFactory.load().getString(Strings.configPosModel)

  val posPipeline = new PosPipeline(spark, posModel)

  override def startTraining(path: Option[String] = None): PipelineModel = {

    val posPipeline_ = posPipeline
    val articlesWithText_ = articlesWithText

    posPipeline_.train(articlesWithText_, path)

  }

  //TODO divide in 2 methods, 1 that uses annotate method, one that uses run method and get rid of option
  override def results(articles: Option[DataFrame], path: String, save: Boolean): DataFrame = {

    val posPipeline_ = posPipeline
    val articlesWithText_ = articlesWithText
    val replacements_ = replacements
    val dao_ = dao

    val annotatedDf = articles match {
      case None => posPipeline_.annotate(articlesWithText_, path)
      case Some(articles) => posPipeline_.annotate(Conversion.prepareArticlesForPipeline(articles, replacements_), path)
    }

    val finalDf = Conversion.prepareArticlesForSaving(annotatedDf, spark)

    if(save){
      dao_.writeArticles(finalDf)
    }

    finalDf
  }
}
