package pipeline

import daos.memory.InMemoryDao
import org.apache.hadoop.mapred.InvalidInputException
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import pipeline.pos.PosPipeline

import java.io.File
import scala.reflect.io.Directory

class PosPipelineTest extends AnyFunSuite{

  val spark: SparkSession = SparkSession
    .builder()
    .appName("SparkNLPPlayground")
    .master("local[*]")
    .config("spark.executor.memory", "12g")
    .config("spark.driver.memory", "12g")
    .getOrCreate()

  val dao = new InMemoryDao(spark,
    "src/test/resources/inMemoryArticles",
    "src/test/resources/writeTest")

  val testArticles = dao.getNewsArticles(Some(10))
  val posPipeline = new PosPipeline(spark, "src/main/resources/models/pos_ud_hdt_de_2.0.8_2.4_1561232528570")
  val destination = "src/test/resources/writeModelTest"

  test("runPipeline should return DataFrame with necessary columns"){
    val annotated = posPipeline.runPipeline(testArticles)
    assert(annotated.isInstanceOf[DataFrame])
    val columns = annotated.columns
    assert(columns.contains("_id"))
    assert(columns.contains("long_url"))
    assert(columns.contains("crawl_time"))
    assert(columns.contains("text"))
    assert(columns.contains("document"))
    assert(columns.contains("sentence"))
    assert(columns.contains("token"))
    assert(columns.contains("normalized"))
    assert(columns.contains("pos"))
    assert(columns.contains("lemma"))
  }

  test("train should return PipelineModel"){
    val model = posPipeline.train(testArticles)
    assert(model.isInstanceOf[PipelineModel])
  }

  test("train with write Option on should write model to given destination"){
    val file = new File(destination)
    if(file.exists()){
      new Directory(file).deleteRecursively()
    }
    posPipeline.train(testArticles, Option(destination))
    assert(file.exists())
  }

  test("train without write Option should not write model to given destination"){
    val currentTime = System.currentTimeMillis()
    posPipeline.train(testArticles, None)
    val file = new File(destination)
    if(file.exists()){
      val modifiedTime = file.lastModified()
      assert(currentTime>modifiedTime)
    } else {
      assertThrows[InvalidInputException]{
        PipelineModel.load(destination)
      }
    }
  }

  test("annotate should return annotated DataFrame with necessary columns"){
    val annotated = posPipeline.annotate(testArticles, destination)
    assert(annotated.isInstanceOf[DataFrame])
    val columns = annotated.columns
    assert(columns.contains("_id"))
    assert(columns.contains("long_url"))
    assert(columns.contains("crawl_time"))
    assert(columns.contains("text"))
    assert(columns.contains("document"))
    assert(columns.contains("sentence"))
    assert(columns.contains("token"))
    assert(columns.contains("normalized"))
    assert(columns.contains("pos"))
    assert(columns.contains("lemma"))
  }
}
