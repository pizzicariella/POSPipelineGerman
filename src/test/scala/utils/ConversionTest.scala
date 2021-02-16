package utils

import org.apache.spark.sql.types.{ArrayType, FloatType, IntegerType, LongType, MapType, StringType, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class ConversionTest extends AnyFunSuite{

  val spark: SparkSession = SparkSession
    .builder()
    .appName("POSPipelineGerman")
    .master("local[*]")
    .config("spark.executor.memory", "12g")
    .config("spark.driver.memory", "12g")
    .getOrCreate()

  import spark.implicits._

  val articlesBeforeConversion = Seq(
    ("testId1", 1575834314719L, "test intro 1", "test title 1", "test text 1", "www.test/long.de"),
    ("testId2", 1575834315000L, "test intro 2", "test title 2", "test text 2", "www.test/long.de"),
    ("testId3", 1575834315067L, "test intro 3", "test title 3", "test text 3", "www.test/long.de")
  ).toDF("_id",  "crawl_time", "intro", "title", "text", "long_url")

  val articlesAfterPipeline = Seq(
    Row("testId1", 1575834314719L, "Das ist ein Test Text.", "www.test/long.de",
      Array(Row("document", 0, 21, "Das ist ein Test Text.", Map("meta" -> "meta1"), Array(0.2F))),
      Array(Row("sentence", 0, 21, "Das ist ein Test Text.", Map("meta" -> "meta1"), Array(0.2F))),
      Array(Row("token", 0, 2, "Das", Map("meta" -> "meta1"), Array(0.2F)),
        Row("token", 4, 6, "ist", Map("meta" -> "meta1"), Array(0.2F)),
        Row("token", 8, 10, "ein", Map("meta" -> "meta1"), Array(0.2F)),
        Row("token", 12, 15, "Test", Map("meta" -> "meta1"), Array(0.2F)),
        Row("token", 17, 20, "Text", Map("meta" -> "meta1"), Array(0.2F))),
      Array(Row("normalized", 0, 2, "Das", Map("meta" -> "meta1"), Array(0.2F)),
        Row("normalized", 4, 6, "ist", Map("meta" -> "meta1"), Array(0.2F)),
        Row("normalized", 8, 10, "ein", Map("meta" -> "meta1"), Array(0.2F)),
        Row("normalized", 12, 15, "Test", Map("meta" -> "meta1"), Array(0.2F)),
        Row("normalized", 17, 20, "Text", Map("meta" -> "meta1"), Array(0.2F))),
      Array(Row("pos", 0, 2, "PRON", Map("meta" -> "meta1"), Array(0.2F)),
        Row("pos", 4, 6, "VERB", Map("meta" -> "meta1"), Array(0.2F)),
        Row("pos", 8, 10, "DET", Map("meta" -> "meta1"), Array(0.2F)),
        Row("pos", 12, 15, "NOUN", Map("meta" -> "meta1"), Array(0.2F)),
        Row("pos", 17, 20, "NOUN", Map("meta" -> "meta1"), Array(0.2F))),
      Array(Row("lemma", 0, 2, "das", Map("meta" -> "meta1"), Array(0.2F)),
        Row("lemma", 4, 6, "sein", Map("meta" -> "meta1"), Array(0.2F)),
        Row("lemma", 8, 10, "ein", Map("meta" -> "meta1"), Array(0.2F)),
        Row("lemma", 12, 15, "Test", Map("meta" -> "meta1"), Array(0.2F)),
        Row("lemma", 17, 20, "Text", Map("meta" -> "meta1"), Array(0.2F))))
  )

  val scheme = new StructType()
    .add("_id", StringType)
    .add("crawl_time", LongType)
    .add("text", StringType)
    .add("long_url", StringType)
    .add("document", ArrayType(new StructType()
      .add("annotatorType", StringType)
      .add("begin", IntegerType)
      .add("end", IntegerType)
      .add("result", StringType)
      .add("metadata", MapType(StringType, StringType))
      .add("embeddings", ArrayType(FloatType))))
    .add("sentence", ArrayType(new StructType()
      .add("annotatorType", StringType)
      .add("begin", IntegerType)
      .add("end", IntegerType)
      .add("result", StringType)
      .add("metadata", MapType(StringType, StringType))
      .add("embeddings", ArrayType(FloatType))))
    .add("token", ArrayType(new StructType()
      .add("annotatorType", StringType)
      .add("begin", IntegerType)
      .add("end", IntegerType)
      .add("result", StringType)
      .add("metadata", MapType(StringType, StringType))
      .add("embeddings", ArrayType(FloatType))))
    .add("normalized", ArrayType(new StructType()
      .add("annotatorType", StringType)
      .add("begin", IntegerType)
      .add("end", IntegerType)
      .add("result", StringType)
      .add("metadata", MapType(StringType, StringType))
      .add("embeddings", ArrayType(FloatType))))
    .add("pos", ArrayType(new StructType()
      .add("annotatorType", StringType)
      .add("begin", IntegerType)
      .add("end", IntegerType)
      .add("result", StringType)
      .add("metadata", MapType(StringType, StringType))
      .add("embeddings", ArrayType(FloatType))))
    .add("lemma", ArrayType(new StructType()
      .add("annotatorType", StringType)
      .add("begin", IntegerType)
      .add("end", IntegerType)
      .add("result", StringType)
      .add("metadata", MapType(StringType, StringType))
      .add("embeddings", ArrayType(FloatType))))

  val articlesAfterPipelineDf = spark.createDataFrame(spark.sparkContext.parallelize(articlesAfterPipeline), scheme)

  //val replacements = Seq((" ", " "), ("(?<=[^A-Z\\d])\\b\\.\\b", ". "))

  test("prepareArticlesForPipeline should create new text column and drop title and intro"){
    val result = Conversion.prepareArticlesForPipeline(articlesBeforeConversion)
    assert(result.isInstanceOf[DataFrame])
    val columns = result.columns
    assert(columns.contains("text"))
    assert(!columns.contains("title"))
    assert(!columns.contains("intro"))
    val firstRow = result.head()
    val text = firstRow.getString(2)
    val firstRowBeforeConversion = articlesBeforeConversion.head
    val title = firstRowBeforeConversion.getString(3)
    assert(text.contains(title))
    val intro = firstRowBeforeConversion.getString(2)
    assert(text.contains(intro))
    val textBefore = firstRowBeforeConversion.getString(4)
    assert(text.contains(textBefore))
    assert(text.contains(" $§$ "))
  }

  test("prepareArticlesForPipeline should remove empty text strings"){
    val articlesWithBrokenArticle = Seq(
      ("testId1", 1575834314719L, "test intro 1", "test title 1", "test text 1", "www.test/long.de"),
      ("testId2", 1575834315000L, "test intro 2", "test title 2", "test text 2", "www.test/long.de"),
      ("testId3", 1575834315067L, null, null, null, "www.test/long.de")
    ).toDF("_id",  "crawl_time", "intro", "title", "text", "long_url")
    val numArticles = articlesWithBrokenArticle.count()
    val result = Conversion.prepareArticlesForPipeline(articlesWithBrokenArticle)
    assert(result.count() === numArticles-1)
  }

  val result = Conversion.prepareArticlesForSaving(articlesAfterPipelineDf)

  test("prepareArticlesForSaving should return DataFrame with relevant AnnotatedArticleColumns only"){

    val columns = result.columns
    assert(columns.contains("_id"))
    assert(columns.contains("long_url"))
    assert(columns.contains("crawl_time"))
    assert(columns.contains("text"))
    assert(columns.contains("pos"))
    assert(columns.contains("pos_percentage"))
    assert(columns.contains("lemma"))
    assert(!columns.contains("sentence"))
    assert(!columns.contains("document"))
    assert(!columns.contains("normalized"))
  }

  test("prepareArticlesForSaving drops nested columns"){
    val sel = result.select("pos.begin")
    assert(sel.isInstanceOf[DataFrame])
    assertThrows[org.apache.spark.sql.AnalysisException]{result.select("pos.embeddings")}
    assertThrows[org.apache.spark.sql.AnalysisException]{result.select("pos.annotatorType")}

    val sel2 = result.select("lemma.beginToken")
    assert(sel2.isInstanceOf[DataFrame])
    assertThrows[org.apache.spark.sql.AnalysisException]{result.select("lemma.embeddings")}
    assertThrows[org.apache.spark.sql.AnalysisException]{result.select("lemma.annotatorType")}
  }

  test("prepareArticlesForSaving should calculate posPercentage correctly"){
    result.printSchema()
    result.show()
    val percentages = result.select("pos_percentage").head().getSeq[Row](0)
      .map(pp => (pp.getString(0), pp.getDouble(1))).toMap
    val pron = percentages.getOrElse("PRON", "empty")
    val verb = percentages.getOrElse("VERB", "empty")
    val det = percentages.getOrElse("DET", "empty")
    val noun = percentages.getOrElse("NOUN", "empty")

    assert(pron === 0.2)
    assert(verb === 0.2)
    assert(det === 0.2)
    assert(noun === 0.4)
  }
}
