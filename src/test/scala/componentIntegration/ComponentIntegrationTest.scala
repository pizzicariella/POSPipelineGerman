package componentIntegration

import daos.memory.FileDao
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import pipeline.pos.PosPipeline
import utils.Conversion
import java.io.File
import scala.reflect.io.Directory

class ComponentIntegrationTest extends AnyFunSuite{

  val spark: SparkSession = SparkSession
    .builder()
    .appName("POSPipelineGerman")
    .master("local[*]")
    .config("spark.executor.memory", "12g")
    .config("spark.driver.memory", "12g")
    .getOrCreate()

  val articleFile = "src/test/resources/inMemoryArticles"
  val path = "src/test/resources/writeModelTest"
  val destination = "src/test/resources/writeTest"
  val dao = new FileDao(spark, articleFile, destination)
  val pipeline = new PosPipeline(spark)
  val articles = dao.getNewsArticles(Some(20))
  val convertedArticles = Conversion.prepareArticlesForPipeline(articles)

  test("pipeline will run successfully after files are read from source and converted"){
    val annotatedDf = pipeline.runPipeline(convertedArticles)
    assert(annotatedDf.isInstanceOf[DataFrame])
  }

  test("articles can be converted and saved successfully after running pipeline"){
    val annotatedDf = pipeline.runPipeline(convertedArticles)
    val preparedDf = Conversion.prepareArticlesForSaving(annotatedDf)
    assert(preparedDf.isInstanceOf[DataFrame])
    val destFile = new File(destination)
    if(destFile.exists()){
      new Directory(new File(destination)).deleteRecursively()
    }
    dao.writeAnnotatedArticles(preparedDf)
    assert(new File(destination).exists())
  }
}
