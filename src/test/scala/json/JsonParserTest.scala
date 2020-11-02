package json

import json.JsonParser.parseDocumentText
import org.scalatest.funsuite.AnyFunSuite

import scala.io.Source

class JsonParserTest extends AnyFunSuite{

  val pathToTestFile = "src/test/resources/testArticle.json"
  val json = readTestFile(pathToTestFile)
  val columns = Array("text", "intro", "title")
  val title = "Delhi - Kurzschluss wohl Ursache für Brand mit 43 Toten"

  test("parseDocumentText should parse specified columns"){
    val parsedJson = parseDocumentText(json, columns)
    assert(parsedJson.contains("text"))
    assert(parsedJson.contains("intro"))
    assert(parsedJson.contains("title"))
  }

  test("parseDocumentText should not parse other columns"){
    val parsedJson = parseDocumentText(json, columns)
    assert(!parsedJson.contains("keywords"))
  }

  test("content of column ist parsed correctly"){
    val parsedJson = parseDocumentText(json, columns)
    assert(parsedJson.get("title") === Some(title))
  }

  def readTestFile(path: String): String = {
    val bufferedSource = Source.fromFile(path)
    val json = bufferedSource.getLines().next()
    bufferedSource.close()
    json
  }

}
