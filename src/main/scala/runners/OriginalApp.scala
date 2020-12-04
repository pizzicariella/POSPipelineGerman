package runners

import com.typesafe.config.ConfigFactory
import daos.db.DbDao
import model.{AnalysedArticle, PosAnnotation, Strings}
import org.apache.spark.sql.{Row, SparkSession}
import pipeline.pos.PosPipeline
import utils.Conversion

object OriginalApp {
  def main(args: Array[String]): Unit = {

    val userName = ConfigFactory.load().getString(Strings.dbConfigUser)
    val pw = ConfigFactory.load().getString(Strings.dbConfigPw)
    val serverAddress = ConfigFactory.load().getString(Strings.dbConfigServer)
    val port = ConfigFactory.load().getString(Strings.dbConfigPort)
    val db = ConfigFactory.load().getString(Strings.dbConfigDb)
    val collectionName = ConfigFactory.load().getString(Strings.dbConfigCollection)

    val targetUserName = ConfigFactory.load().getString(Strings.targetDbConfigUser)
    val targetPw = ConfigFactory.load().getString(Strings.targetDbConfigPw)
    val targetServerAddress = ConfigFactory.load().getString(Strings.targetDbConfigServer)
    val targetPort = ConfigFactory.load().getString(Strings.targetDbConfigPort)
    val targetDb = ConfigFactory.load().getString(Strings.targetDbConfigDb)
    val targetCollectionName = ConfigFactory.load().getString(Strings.targetDbConfigCollection)

    val posModel = ConfigFactory.load().getString(Strings.configPosModel)

    val sc: SparkSession = SparkSession
      .builder()
      .appName(Strings.sparkParamsAppName)
      .master(Strings.sparkParamsLocal)
      .config(Strings.sparkConigExecuterMemory, Strings.sparkParamsMemory)
      .config(Strings.sparkConfigDriverMemory, Strings.sparkParamsMemory)
      .getOrCreate()

    val dao = new DbDao(userName, pw, serverAddress, port, db)
    val articles = dao.getNewsArticles(Some(200), collectionName)
    dao.close()

    val articlesWithText = Conversion.prepareArticles(articles)

    val posPipeline = new PosPipeline(sc, posModel)
    val replacements = Map(Strings.replacePatternSpecialWhitespaces -> Strings.replacementWhitespaces,
      Strings.replacePatternMissingWhitespaces -> Strings.replacementMissingWhitespaces)
    val annotations = posPipeline.runPipeline(articlesWithText, replacements)

    val metaTextPosDf = annotations.select(Strings.columnId,
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

    val targetDao = new DbDao(targetUserName, targetPw, targetServerAddress, targetPort, targetDb)
    analysedArticles.foreach(article => targetDao.writeArticle(article, targetCollectionName))
    targetDao.close()
  }
}
