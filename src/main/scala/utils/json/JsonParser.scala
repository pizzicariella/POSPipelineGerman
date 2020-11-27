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
      case None => throw new NoSuchElementException("The column " + Strings.columnId + " does not exist.")
    }, data.get(Strings.columnLongUrl) match {
      case Some(url: JsString) => url match {
        case JsString(value) => value
      }
      case None => throw new NoSuchElementException("The column " + Strings.columnLongUrl + " does not exist.")
    }, data.get(Strings.columnCrawlTime) match {
      case Some(date: JsObject) => date match {
        case JsObject(fields) if fields contains(Strings.fieldDate) => fields(Strings.fieldDate) match {
          case JsNumber(value) => value.toString()
        }
      }
      case None => throw new NoSuchElementException("The column " + Strings.columnCrawlTime + " does not exist.")
    }, data.get(Strings.columnTitle) match {
      case Some(title: JsString) => title match {
        case JsString(value) => value
      }
      case None => throw new NoSuchElementException("The column " + Strings.columnTitle + " does not exist.")
    }, data.get(Strings.columnIntro) match {
      case Some(intro: JsString) => intro match {
        case JsString(value) => value
      }
      case None => throw new NoSuchElementException("The column " + Strings.columnIntro + " does not exist.")
    }, data.get(Strings.columnText) match {
      case Some(text: JsString) => text match {
        case JsString(value) => value
      }
      case None => throw new NoSuchElementException("The column " + Strings.columnText + " does not exist.")
    })

    /*columns.map(columnName => (columnName,
      data
        .getOrElse(columnName, throw new NoSuchElementException("The column " + columnName + " does not exist.")) match {
        case JsObject(fields) if fields.contains("$date") => fields("$date") match {
          case JsNumber(value) => value.toString()
        }
        case JsObject(fields) if fields.contains("$oid") => fields("$oid") match {
          case JsString(value) => value
        }
        case JsString(value) => value
      })).toMap*/

    /*data.filter(entry => columns.contains(entry._1))
        .map(entry => (entry._1, entry._2 match {
            case JsString(value) => value
            case _ => ""
        }))*/

    //previously parse doc text method
    /*columns.map(columnName =>
      data.getOrElse(columnName, throw new NoSuchElementException("The column "+columnName+" does not exist.")) match {
        case JsString(value) => value
        case _ => ""
      })
      .reduce((s1, s2) => s1+" "+s2)*/
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
