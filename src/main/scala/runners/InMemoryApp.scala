package runners

import com.typesafe.config.ConfigFactory
import daos.memory.InMemoryDao
import meta.ExtraInformation
import model.{AnnotatedArticle, PosAnnotation, Strings}
import org.apache.spark.sql.functions.{col, concat, explode, row_number, translate}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}
import pipeline.pos.PosPipeline
import utils.Conversion

object InMemoryApp {
  def main(args: Array[String]): Unit = {

    val articleFile = ConfigFactory.load().getString("app.inmemoryfile_test")
    val targetFile = ConfigFactory.load().getString("app.target_inmemoryfile")
    val posModel = ConfigFactory.load().getString(Strings.configPosModel)

    val spark: SparkSession = SparkSession
      .builder()
      .appName(Strings.sparkParamsAppName)
      .master(Strings.sparkParamsLocal)
      .config(Strings.sparkConigExecuterMemory, Strings.sparkParamsMemory)
      .config(Strings.sparkConfigDriverMemory, Strings.sparkParamsMemory)
      .getOrCreate()

    val dao = new InMemoryDao(spark)
    val articles = dao.getNewsArticles(Some(200), articleFile)
    val replacements = Seq((Strings.replacePatternSpecialWhitespaces, Strings.replacementWhitespaces),
      (Strings.replacePatternMissingWhitespaces, Strings.replacementMissingWhitespaces))
    val articlesWithText = Conversion.prepareArticles(articles, replacements)

    val posPipeline = new PosPipeline(spark, posModel)

    val annotations = posPipeline.runPipeline(articlesWithText)

    val metaTextPosDf = annotations.select(Strings.columnId,
      Strings.columnLongUrl,
      Strings.columnCrawlTime,
      Strings.columnText,
      Strings.columnPos)

    val exploded = metaTextPosDf.withColumn("pos", explode(col("pos")))
    //val index = exploded.schema.fieldIndex("pos")
    //val posSchema = exploded.schema(index).dataType.asInstanceOf[StructType].drop(0).drop(4).drop(5)
    //val finalDf = exploded.withColumn("pos", posSchema)
    //metaTextPosDf.explain(true)
    //metaTextPosDf.select("pos").select("element").select("begin")
    exploded.printSchema()
    metaTextPosDf.show(1,false)

    /*val annotatedArticles = metaTextPosDf
      .rdd
      .map(row => {
        val posList = row.getSeq[Row](4)
          .map(innerRow => PosAnnotation(innerRow.getInt(1),
            innerRow.getInt(2),
            innerRow.getString(3))
          ).toList

        AnnotatedArticle(row.getString(0),
          row.getString(1),
          row.getString(2),
          row.getString(3),
          posList,
          ExtraInformation.getPosPercentage(posList)
        )
      }
      )*/

    //dao.writeArticles(annotatedArticles, targetFile)
  }
}
