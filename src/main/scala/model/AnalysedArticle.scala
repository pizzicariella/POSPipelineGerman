package model

import spray.json.{DefaultJsonProtocol, JsArray, JsNumber, JsObject, JsString, JsValue, RootJsonFormat}

case class AnalysedArticle(id: String,
                           longUrl: String,
                           crawlTime: BigDecimal,
                           text: String,
                           annosPos: Seq[(Int, Int, String)]) {
  def annosPosAsJsArray: Vector[JsArray] = annosPos.map(anno => JsArray(JsNumber(anno._1),
                                                                JsNumber(anno._2),
                                                                JsString(anno._3))).toVector

  override def toString: String = Strings.analysedArticleString(id, longUrl, crawlTime, text, annosPos)

}

object AnalysedArticleJsonProtocol extends DefaultJsonProtocol{

  implicit object AnalysedArticleJsonFormat extends RootJsonFormat[AnalysedArticle] {

    override def read(json: JsValue): AnalysedArticle = ???

    override def write(analysedArticle: AnalysedArticle): JsObject = JsObject(
        Strings.columnId -> JsObject(Map(Strings.fieldId -> JsString(analysedArticle.id))),
        Strings.columnLongUrl -> JsString(analysedArticle.longUrl),
        Strings.columnCrawlTime -> JsObject(Map(Strings.fieldDate -> JsNumber(analysedArticle.crawlTime))),
        Strings.columnText -> JsString(analysedArticle.text),
        Strings.columnAnnosPos -> JsArray(analysedArticle.annosPosAsJsArray)
      )
  }
}


