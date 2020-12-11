package model

import spray.json.{DefaultJsonProtocol, JsArray, JsNumber, JsObject, JsString, JsValue, RootJsonFormat}

case class AnnotatedArticle(id: String,
                           longUrl: String,
                           crawlTime: String,
                           text: String,
                           annotationsPos: List[PosAnnotation],
                           tagPercentage: List[PosPercentage]) {

  def annotationsPosAsJsObject: Vector[JsObject] = annotationsPos
    .map(anno => PosAnnotationJsonProtocol.PosAnnotationJsonFormat.write(anno))
    .toVector

  override def toString: String = Strings.analysedArticleString(id, longUrl, crawlTime, text, annotationsPos)
}

object AnalysedArticleJsonProtocol extends DefaultJsonProtocol{

  implicit object AnnotatedArticleJsonFormat extends RootJsonFormat[AnnotatedArticle] {

    override def read(json: JsValue): AnnotatedArticle = ???

    override def write(annotatedArticle: AnnotatedArticle): JsObject = JsObject(
        "_id" -> JsString(annotatedArticle.id),
        "longUrl" -> JsString(annotatedArticle.longUrl),
        "crawlTime" -> JsString(annotatedArticle.crawlTime),
        Strings.columnText -> JsString(annotatedArticle.text),
        "annotationsPos" -> JsArray(annotatedArticle.annotationsPosAsJsObject)
        //"tagsPercentage" -> JsObject(annotatedArticle.tagPercentage.toMap.mapValues(percentage => JsNumber(percentage)))
      )
  }

}


