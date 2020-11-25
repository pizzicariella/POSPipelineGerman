package utils.json

import spray.json.{DefaultJsonProtocol, JsArray, JsNumber, JsObject, JsString, JsValue, RootJsonFormat}

case class AnalysedArticle(id: String,
                           longUrl: String,
                           crawlTime: BigDecimal,
                           text: String,
                           annosPos: Seq[(Int, Int, String)]) {
  def annosPosAsJsArray: Vector[JsArray] = annosPos.map(anno => JsArray(JsNumber(anno._1),
                                                                JsNumber(anno._2),
                                                                JsString(anno._3))).toVector

}

object AnalysedArticleJsonProtocol extends DefaultJsonProtocol{

  implicit object AnalysedArticleJsonFormat extends RootJsonFormat[AnalysedArticle] {

    override def read(json: JsValue): AnalysedArticle = ???

    override def write(analysedArticle: AnalysedArticle): JsObject = JsObject(
        "_id" -> JsObject(Map("$oid" -> JsString(analysedArticle.id))),
        "longUrl" -> JsString(analysedArticle.longUrl),
        "crawlTime" -> JsObject(Map("$date" -> JsNumber(analysedArticle.crawlTime))),
        "text" -> JsString(analysedArticle.text),
        "annosPos" -> JsArray(analysedArticle.annosPosAsJsArray)
      )
  }
}


