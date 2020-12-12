package utils.json

import model.{AnnotatedArticle, PosAnnotation, PosPercentage}
import org.scalatest.funsuite.AnyFunSuite

//TODO delete if not needed any longer
class JsonComposerTest extends AnyFunSuite{

  val testArticle = AnnotatedArticle("test_1",
    "www.test.de",
    "1574281189000",
    "Das Landeskriminalamt (LKA) und die Münchner Polizei fanden in der mit Chemikalien vollgepackten Dreizimmerwohnung in Sichtweite des Gondrellplatzes nämlich bisher mehr als 50 Kilogramm pyrotechnische Gegenstände und eine Vielzahl laut LKA vermutlich illegaler Feuerwerkskörper.",
    List(PosAnnotation(0,2,"DET"),PosAnnotation(4,20,"NOUN")), List(PosPercentage("DET", 0.5), PosPercentage("Noun", 0.5)))

  val jsonString = "{\"tagsPercentage\":{\"DET\":0.5,\"Noun\":0.5}," +
    "\"annotationsPos\":[{\"begin\":0,\"end\":2,\"tag\":\"DET\"},{\"begin\":4,\"end\":20,\"tag\":\"NOUN\"}],"+
    "\"longUrl\":\"www.test.de\"," +
    "\"_id\":\"test_1\"," +
    "\"text\":\"Das Landeskriminalamt (LKA) und die Münchner Polizei fanden in der mit Chemikalien vollgepackten Dreizimmerwohnung in Sichtweite des Gondrellplatzes nämlich bisher mehr als 50 Kilogramm pyrotechnische Gegenstände und eine Vielzahl laut LKA vermutlich illegaler Feuerwerkskörper.\"," +
    "\"crawlTime\":\"1574281189000\"}"

  test("json String is composed correctly"){
    val composedJson = JsonComposer.composeAnalysedArticleJson(testArticle)
    assert(composedJson === jsonString)
  }


}
