package utils.json

import model.AnalysedArticle
import org.scalatest.funsuite.AnyFunSuite

class JsonComposerTest extends AnyFunSuite{

  val testArticle = AnalysedArticle("test_1",
    "www.test.de",
    BigDecimal("1574281189000"),
    "Das Landeskriminalamt (LKA) und die Münchner Polizei fanden in der mit Chemikalien vollgepackten Dreizimmerwohnung in Sichtweite des Gondrellplatzes nämlich bisher mehr als 50 Kilogramm pyrotechnische Gegenstände und eine Vielzahl laut LKA vermutlich illegaler Feuerwerkskörper.",
    Seq((0,2,"DET"),(4,20,"NOUN")))

  val jsonString = "{\"_id\":{\"$oid\":\"test_1\"}," +
    "\"text\":\"Das Landeskriminalamt (LKA) und die Münchner Polizei fanden in der mit Chemikalien vollgepackten Dreizimmerwohnung in Sichtweite des Gondrellplatzes nämlich bisher mehr als 50 Kilogramm pyrotechnische Gegenstände und eine Vielzahl laut LKA vermutlich illegaler Feuerwerkskörper.\"," +
    "\"long_url\":\"www.test.de\"," +
    "\"annosPos\":[[0,2,\"DET\"],[4,20,\"NOUN\"]],"+
    "\"crawl_time\":{\"$date\":1574281189000}}"

  test("json String is composed correctly"){
    val composedJson = JsonComposer.composeAnalysedArticleJson(testArticle)
    assert(composedJson === jsonString)
  }


}
