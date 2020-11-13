package pipeline

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import pipeline.pos.PosPipeline

class PosPipelineTest extends AnyFunSuite{

  val sc: SparkSession = SparkSession
    .builder()
    .appName("SparkNLPPlayground")
    .master("local[*]")
    .config("spark.executor.memory", "12g")
    .config("spark.driver.memory", "12g")
    .getOrCreate()

  import sc.implicits._

  val text = "Im Jahr 1997 starben in einem Kino 59 Menschen, die meisten Besucher erstickten, nachdem ein Transformator explodiert war."
  val data = Seq(text).toDF("text")
  val posPipeline = new PosPipeline(sc)
  val pattern = "[ ,\\.]"
  val replacement = " "
  val textReplaced = "Im Jahr 1997 starben in einem Kino 59 Menschen  die meisten Besucher erstickten  nachdem ein Transformator explodiert war "

  test("replaceSplitChars should replace according to pattern"){
    val replacedDf = posPipeline.replaceSplitChars(data, Some(pattern), Some(replacement))
    val replacedTextByMethod = replacedDf
      .select("text")
      .map(row => row.getString(0))
      .collect()
      .head
    assert(replacedTextByMethod === textReplaced)
  }

  test("replaceSplitChars should not replace split chars if None is given"){
    val unreplacedDf = posPipeline.replaceSplitChars(data, None, None)
    val unreplacedText = unreplacedDf
      .select("text")
      .map(row => row.getString(0))
      .collect()
      .head
    assert(unreplacedText === text)
  }
}
