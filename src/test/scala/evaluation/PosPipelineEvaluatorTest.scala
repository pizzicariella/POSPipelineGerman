package evaluation

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, StringType, StructType}
import org.scalatest.funsuite.AnyFunSuite

class PosPipelineEvaluatorTest extends AnyFunSuite{

  val spark: SparkSession = SparkSession
    .builder()
    .appName("POSPipelineGerman")
    .master("local[*]")
    .config("spark.executor.memory", "12g")
    .config("spark.driver.memory", "12g")
    .getOrCreate()

  val testArticles = Seq(
    Row("testId1",
      1575834314719L,
      "Dies ist ein Test Text.",
      "www.test/long.de",
      Array(Row(0,3,"PRON"), Row(5,7,"AUX"), Row(9,11, "DET"), Row(13,16, "VERB"), Row(18,21, "NOUN")),
      Array(Row(0,3,"dies"), Row(5,7,"sein"), Row(9,11, "ein"), Row(13,16, "testen"), Row(18,21, "texten"))),
    Row("testId2",
      1575834315000L,
      "Dies ist noch ein Test Text.",
      "www.test/long.de",
      Array(Row(0,3,"PRON"), Row(5,7,"AUX"), Row(9,12,"PART"), Row(13,15, "DET"), Row(17,20, "VERB"), Row(22,25, "NOUN")),
      Array(Row(0,3,"dies"), Row(5,7,"sein"), Row(9,12,"noch"), Row(13,15, "ein"), Row(17,20, "testen"), Row(22,25, "Text")))
  )

  val goldStandard = Seq(
    Row("testId1",
      1575834314719L,
      "Dies ist ein Test Text.",
      "www.test/long.de",
      Array(Row(0,3,"PRON"), Row(5,7,"AUX"), Row(9,11, "DET"), Row(13,16, "NOUN"), Row(18,21, "NOUN")),
      Array(Row(0,3,"dies"), Row(5,7,"sein"), Row(9,11, "ein"), Row(13,16, "Test"), Row(18,21, "Text"))),
    Row("testId2",
      1575834315000L,
      "Dies ist noch ein Test Text.",
      "www.test/long.de",
      Array(Row(0,3,"PRON"), Row(5,7,"AUX"), Row(9,12,"ADV"), Row(13,15, "DET"), Row(17,20, "NOUN"), Row(22,25, "NOUN")),
      Array(Row(0,3,"dies"), Row(5,7,"sein"), Row(9,12,"noch"), Row(13,15, "ein"), Row(17,20, "Test"), Row(22,25, "Text")))
  )

  val scheme = new StructType()
    .add("_id", StringType)
    .add("crawl_time", LongType)
    .add("text", StringType)
    .add("long_url", StringType)
    .add("pos", ArrayType(new StructType()
      .add("begin", IntegerType)
      .add("end", IntegerType)
      .add("tag", StringType)))
    .add("lemma", ArrayType(new StructType()
      .add("beginToken", IntegerType)
      .add("endToken", IntegerType)
      .add("result", StringType)))

  val testArticlesDf = spark.createDataFrame(spark.sparkContext.parallelize(testArticles), scheme)
  val goldStandardDf = spark.createDataFrame(spark.sparkContext.parallelize(goldStandard), scheme)

  test("evaluateModel should calculate accuracy correctly"){
    val evaluator = new PosPipelineEvaluator()
    val accuracyDf = evaluator.evaluateModel(testArticlesDf, goldStandardDf)
    val resultTestArticle1 = accuracyDf
      .select("accuracy_pos", "accuracy_lemma")
      .where("_id='testId1'")
    val resultTestArticle2 = accuracyDf
      .select("accuracy_pos", "accuracy_lemma")
      .where("_id='testId2'")
    val posAccuracyTestArticle1 = resultTestArticle1.head().getDouble(0)
    val lemmaAccuracyTestArticle1 = resultTestArticle1.head().getDouble(1)
    val posAccuracyTestArticle2 = resultTestArticle2.head().getDouble(0)
    val lemmaAccuracyTestArticle2 = resultTestArticle2.head().getDouble(1)
    assert(posAccuracyTestArticle1 == 0.8)
    assert(lemmaAccuracyTestArticle1 == 0.6)
    assert(posAccuracyTestArticle2 == 0.6666666666666666)
    assert(lemmaAccuracyTestArticle2 == 0.8333333333333334)
  }
}
