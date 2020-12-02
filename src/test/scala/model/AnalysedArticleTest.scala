package model

import org.scalatest.funsuite.AnyFunSuite
import spray.json.JsArray

class AnalysedArticleTest extends AnyFunSuite{

  val testArticle = AnalysedArticle("test_1",
    "www.test.de",
    BigDecimal("1574281189000"),
    "Das Landeskriminalamt (LKA) und die Münchner Polizei fanden in der mit Chemikalien vollgepackten Dreizimmerwohnung in Sichtweite des Gondrellplatzes nämlich bisher mehr als 50 Kilogramm pyrotechnische Gegenstände und eine Vielzahl laut LKA vermutlich illegaler Feuerwerkskörper.",
    Seq((0,2,"DET"),(4,20,"NOUN")))

  test("toString returns correct String"){
    val testArticleString = "(test_1, " +
      "www.test.de, 1574281189000, " +
      "Das Landeskriminalamt (LKA) und die Münchner Polizei fanden in der mit Chemikalien vollgepackten Dreizimmerwohnung in Sichtweite des Gondrellplatzes nämlich bisher mehr als 50 Kilogramm pyrotechnische Gegenstände und eine Vielzahl laut LKA vermutlich illegaler Feuerwerkskörper., " +
      "List((0,2,DET), (4,20,NOUN)))"

    assert(testArticleString === testArticle.toString)
  }

  test("annosPosAsJsArray converts annosPos correctly"){
    val vec = testArticle.annosPosAsJsArray
    assert(vec.isInstanceOf[Vector[JsArray]])
    assert(vec.size === 2)
  }

}
