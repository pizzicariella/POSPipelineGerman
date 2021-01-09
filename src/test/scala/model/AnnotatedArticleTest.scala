package model

import org.scalatest.funsuite.AnyFunSuite
import spray.json.JsObject
import java.sql.Timestamp

//TODO update
class AnnotatedArticleTest extends AnyFunSuite{

  val testArticle = AnnotatedArticle("test_1",
    "www.test.de",
    new Timestamp(1574281189000L),
    "Das Landeskriminalamt (LKA) und die Münchner Polizei fanden in der mit Chemikalien vollgepackten Dreizimmerwohnung in Sichtweite des Gondrellplatzes nämlich bisher mehr als 50 Kilogramm pyrotechnische Gegenstände und eine Vielzahl laut LKA vermutlich illegaler Feuerwerkskörper.",
    List(PosAnnotation(0,2,"DET"),PosAnnotation(4,20,"NOUN")), List(PosPercentage("DET", 0.5), PosPercentage("Noun", 0.5)))

  //TODO edit tostring method
  test("toString returns correct String"){
    val testArticleString = "(test_1, " +
      "www.test.de, 1574281189000, " +
      "Das Landeskriminalamt (LKA) und die Münchner Polizei fanden in der mit Chemikalien vollgepackten Dreizimmerwohnung in Sichtweite des Gondrellplatzes nämlich bisher mehr als 50 Kilogramm pyrotechnische Gegenstände und eine Vielzahl laut LKA vermutlich illegaler Feuerwerkskörper., " +
      "List([begin: 0, end: 2, tag: DET], [begin: 4, end: 20, tag: NOUN]))"

    assert(testArticleString === testArticle.toString)
  }

  test("annosPosAsJsObject converts annotationsPos correctly"){
    val vec = testArticle.annotationsPosAsJsObject
    assert(vec.isInstanceOf[Vector[JsObject]])
    assert(vec.size === 2)
  }

}
