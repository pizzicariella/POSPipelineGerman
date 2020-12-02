package utils.json

import model.{NewsArticle, Strings}
import spray.json.DefaultJsonProtocol._
import spray.json._

object JsonParser {

  def parseNewsArticle(json: String): NewsArticle = {

    val jsonAst = json.parseJson
    val data = jsonAst.convertTo[Map[String, JsValue]]

    //TODO add case _ clauses
    NewsArticle(data.get(Strings.columnId) match {
      case Some(id: JsObject) => id match {
        case JsObject(fields) if fields.contains(Strings.fieldId) => fields(Strings.fieldId) match {
          case JsString(value) => value
        }
      }
      case None => throw new NoSuchElementException(Strings.noSuchColumnString(Strings.columnId))
    }, data.get(Strings.columnLongUrl) match {
      case Some(url: JsString) => url match {
        case JsString(value) => value
      }
      case None => throw new NoSuchElementException(Strings.noSuchColumnString(Strings.columnLongUrl))
    }, data.get(Strings.columnCrawlTime) match {
      case Some(date: JsObject) => date match {
        case JsObject(fields) if fields contains(Strings.fieldDate) => fields(Strings.fieldDate) match {
          case JsNumber(value) => value.toString()
          case JsObject(innerFields) if innerFields contains(Strings.fieldNumberLong) =>
              innerFields(Strings.fieldNumberLong) match {
            case JsString(innerValue) => innerValue
          }
        }
      }
      case None => throw new NoSuchElementException(Strings.noSuchColumnString(Strings.columnCrawlTime))
    }, data.get(Strings.columnTitle) match {
      case Some(title: JsString) => title match {
        case JsString(value) => value
      }
      case None => throw new NoSuchElementException(Strings.noSuchColumnString(Strings.columnTitle))
    }, data.get(Strings.columnIntro) match {
      case Some(intro: JsString) => intro match {
        case JsString(value) => value
      }
      case None => throw new NoSuchElementException(Strings.noSuchColumnString(Strings.columnIntro))
    }, data.get(Strings.columnText) match {
      case Some(text: JsString) => text match {
        case JsString(value) => value
      }
      case None => throw new NoSuchElementException(Strings.noSuchColumnString(Strings.columnText))
    })
  }

  def parsePosTags(json: String): List[String] = {
    val jsonAst = json.parseJson
    val data = jsonAst.convertTo[Map[String, JsValue]]
    data.map(entry => entry._2 match {
      case JsArray(elements) => elements.toList.map(element => element.asInstanceOf[JsString].value)
      case _ => List.empty
    })
      .head
  }
}
