package json

import spray.json.DefaultJsonProtocol._
import spray.json._

object JsonParser {

  //TODO: convert non string columns into strings?
  def parseDocumentText(json: String, columns: Array[String]) : String = {
    val jsonAst = json.parseJson
    val data = jsonAst.convertTo[Map[String, JsValue]]

    //TODO need special char to divide title intro text?
    columns.map(columnName =>
      data.getOrElse(columnName, throw new NoSuchElementException("The column "+columnName+" does not exist.")) match {
        case JsString(value) => value
        case _ => ""
      })
      .reduce((s1, s2) => s1+" "+s2)
    //TODO the regex replace could also take place here. Measure whats faster with big amount of articles.

   /* data.filter(entry => columns.contains(entry._1))
        .map(entry => (entry._1, entry._2 match {
            case JsString(value) => value
            case _ => ""
        }))*/
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
