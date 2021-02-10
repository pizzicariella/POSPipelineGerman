package training.pos

import com.typesafe.config.ConfigFactory
import daos.DAO
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.{DataFrame, SparkSession}
import pipeline.pos.PosPipeline
import training.Trainer
import utils.Conversion

class PosTrainer(spark: SparkSession, numArticles: Option[Int], dao: DAO) extends Trainer{

  val replacements = Seq((" ", " "), ("(?<=[^A-Z\\d])\\b\\.\\b", ". "))
  val articlesWithText = Conversion.prepareArticlesForPipeline(dao.getNewsArticles(numArticles), replacements)

  val posModel = ConfigFactory.load().getString("app.pos_tagger_model")

  val posPipeline = new PosPipeline(spark, posModel)

  override def startTraining(path: Option[String] = None): PipelineModel = {

    val posPipeline_ = posPipeline

    posPipeline_.train(articlesWithText, path)

  }

  //TODO divide in 2 methods, 1 that uses annotate method, one that uses run method and get rid of option
  override def results(articles: Option[DataFrame], path: String, save: Boolean): DataFrame = {

    val posPipeline_ = posPipeline
    val replacements_ = replacements
    val dao_ = dao

    val annotatedDf = articles match {
      case None => posPipeline_.annotate(articlesWithText, path)
      case Some(articles) => posPipeline_.annotate(Conversion.prepareArticlesForPipeline(articles, replacements_), path)
    }

    val finalDf = Conversion.prepareArticlesForSaving(annotatedDf)

    if(save){
      dao_.writeAnnotatedArticles(finalDf)
    }

    finalDf
  }
}
