package pipeline

import com.typesafe.config.ConfigFactory
import model.Strings
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

  val posModel = ConfigFactory.load().getString(Strings.configPosModel)
  val text = "Im Jahr 1997 starben in einem Kino 59 Menschen, die meisten Besucher erstickten, nachdem ein Transformator explodiert war.Testsentece."
  val data = Seq(text).toDF("text")
  val posPipeline = new PosPipeline(sc, posModel)
  val replacements = Map(" " -> " ",
    "(?<=[^A-Z\\d])\\b\\.\\b" -> ". ")
  val textReplaced = "Im Jahr 1997 starben in einem Kino 59 Menschen, die meisten Besucher erstickten, nachdem ein Transformator explodiert war. Testsentece."

  test("replace should replace according to pattern"){
    val replacedDf = posPipeline.replace(data, replacements)
    val replacedTextByMethod = replacedDf
      .select("text")
      .map(row => row.getString(0))
      .collect()
      .head
    assert(replacedTextByMethod === textReplaced)
  }

  test("replaceSplitChars should not replace anything if replacement map is empty"){
    val unreplacedDf = posPipeline.replace(data, Map.empty)
    val unreplacedText = unreplacedDf
      .select("text")
      .map(row => row.getString(0))
      .collect()
      .head
    assert(unreplacedText === text)
  }
}
