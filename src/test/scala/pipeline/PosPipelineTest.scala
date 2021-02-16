package pipeline

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

  import spark.implicits._

  val articles = Seq(
    ("testId1", 1575834314719L, "test intro 1", "test title 1", "test text 1", "www.test/long.de"),
    ("testId2", 1575834315000L, "test intro 2", "test title 2", "test text 2", "www.test/long.de"),
    ("testId3", 1575834315067L, "test intro 3", "test title 3", "test text 3", "www.test/long.de")
  ).toDF("_id",  "crawl_time", "intro", "title", "text", "long_url")
  val posPipeline = new PosPipeline(spark, "src/main/resources/models/pos_ud_hdt_de_2.0.8_2.4_1561232528570")
  val destination = "src/test/resources/writeModelTest"

  test("runPipeline should return DataFrame with necessary columns"){
    val annotated = posPipeline.runPipeline(articles)
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
    val model = posPipeline.train(articles)
    assert(model.isInstanceOf[PipelineModel])
  }

  test("train with write Option on should write model to given destination"){
    val file = new File(destination)
    if(file.exists()){
      new Directory(file).deleteRecursively()
    }
    posPipeline.train(articles, Option(destination))
    assert(file.exists())
  }

  test("train without write Option should not write model to given destination"){
    val currentTime = System.currentTimeMillis()
    posPipeline.train(articles)
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
    val annotated = posPipeline.annotate(articles, destination)
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
