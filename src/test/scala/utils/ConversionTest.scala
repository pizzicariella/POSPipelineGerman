package utils

import com.typesafe.config.ConfigFactory
import model.NewsArticle
import org.scalatest.funsuite.AnyFunSuite
import model.Strings
import utils.json.JsonParser

class ConversionTest extends AnyFunSuite{

  val testArticle = NewsArticle("test_1",
    "www.test.de",
    "1574281189000",
    "Das Landeskriminalamt (LKA) und die Münchner Polizei",
    "fanden in der mit Chemikalien vollgepackten Dreizimmerwohnung in Sichtweite des Gondrellplatzes",
    "nämlich bisher mehr als 50 Kilogramm pyrotechnische Gegenstände und eine Vielzahl laut LKA vermutlich illegaler Feuerwerkskörper.")

  val correctlyConverted = ("test_1",
    "www.test.de",
    "1574281189000",
    "Das Landeskriminalamt (LKA) und die Münchner Polizei fanden in der mit Chemikalien vollgepackten Dreizimmerwohnung in Sichtweite des Gondrellplatzes nämlich bisher mehr als 50 Kilogramm pyrotechnische Gegenstände und eine Vielzahl laut LKA vermutlich illegaler Feuerwerkskörper.")

  val newsArticles = FileIO.readJsonFile(ConfigFactory.load().getString(Strings.configTestFile))
    .map(json => JsonParser.parseNewsArticle(json))

  test("switchArticleFormat should convert NewsArticle correctly"){
    val converted = Conversion.switchArticleFormat(testArticle)
    assert(converted === correctlyConverted)
  }

  test("prepareArticles should map Seq of NewsArticles correctly"){
    val convertedArticles = Conversion.prepareArticles(newsArticles)
    assert(convertedArticles.isInstanceOf[Seq[(String, String, String, String)]])
  }

}
