package utils

import daos.memory.FileDao
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.{DataFrame, SparkSession}
import pipeline.pos.PosPipeline

class ConversionTest extends AnyFunSuite{

  val spark: SparkSession = SparkSession
    .builder()
    .appName("POSPipelineGerman")
    .master("local[*]")
    .config("spark.executor.memory", "12g")
    .config("spark.driver.memory", "12g")
    .getOrCreate()

  val dao = new FileDao(spark, "src/test/resources/inMemoryArticles", "none")
  val articlesBeforeConversion = dao.getNewsArticles(Some(10))

  val replacements = Seq((" ", " "), ("(?<=[^A-Z\\d])\\b\\.\\b", ". "))

  test("prepareArticlesForPipeline should create new text column and drop title and intro"){
    val result = Conversion.prepareArticlesForPipeline(articlesBeforeConversion, replacements)
    assert(result.isInstanceOf[DataFrame])
    val columns = result.columns
    assert(columns.contains("text"))
    assert(!columns.contains("title"))
    assert(!columns.contains("intro"))
    val firstRow = result.head()
    val text = firstRow.getString(4)
    val firstRowBeforeConversion = articlesBeforeConversion.head
    val title = firstRowBeforeConversion.getString(6)
    assert(text.contains(title))
    assert(text.contains(" $§$ "))
  }

  test("prepareArticlesForPipeline should replace according to given replacements"){
    val result = Conversion.prepareArticlesForPipeline(articlesBeforeConversion, replacements)
    val testTexts = result.head(3)
    assert(!testTexts(0).getString(4).contains(" "))
    assert(!testTexts(1).getString(4).contains(" "))
    assert(!testTexts(2).getString(4).contains(" "))
    assert(!testTexts(0).getString(4).matches("(?<=[^A-Z\\d])\\b\\.\\b"))
    assert(!testTexts(1).getString(4).matches("(?<=[^A-Z\\d])\\b\\.\\b"))
    assert(!testTexts(2).getString(4).matches("(?<=[^A-Z\\d])\\b\\.\\b"))
  }

  test("prepareArticlesForPipeline should remove empty text strings"){
    val daoForBrokenFile = new FileDao(spark, "src/test/resources/brokenTestFile.json", "none")
    val brokenArticle = daoForBrokenFile.getNewsArticles(Some(1))
    val articlesWithEmptyText = articlesBeforeConversion.union(brokenArticle)
    val numArticles = articlesWithEmptyText.count()
    val result = Conversion.prepareArticlesForPipeline(articlesWithEmptyText, replacements)
    assert(result.count() === numArticles-1)
  }

  test("prepareArticlesForSaving should return DataFrame with relevant AnnotatedArticleColumns only"){
    val posPipeline = new PosPipeline(spark, "src/main/resources/models/pos_ud_hdt_de_2.0.8_2.4_1561232528570")
    val annotated = posPipeline.runPipeline(articlesBeforeConversion)
    val result = Conversion.prepareArticlesForSaving(annotated)
    val columns = result.columns
    assert(columns.contains("_id"))
    assert(columns.contains("long_url"))
    assert(columns.contains("crawl_time"))
    assert(columns.contains("text"))
    assert(columns.contains("pos"))
    assert(columns.contains("posPercentage"))
    assert(columns.contains("lemma"))
    assert(!columns.contains("sentence"))
    assert(!columns.contains("document"))
    assert(!columns.contains("normalized"))
  }

  test("prepareArticlesForSaving should calculate posPercentage correctly"){
    //TODO
  }
}
