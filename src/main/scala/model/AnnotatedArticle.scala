package model

import spray.json.{DefaultJsonProtocol, JsArray, JsObject, JsString, JsValue, RootJsonFormat}
import java.sql.Timestamp


case class AnnotatedArticle(_id: String,
                            longUrl: String,
                            crawlTime: Timestamp,
                            text: String,
                            annotationsPos: List[PosAnnotation],
                            tagPercentage: List[PosPercentage]) {

  //TODO delete?
  def annotationsPosAsJsObject: Vector[JsObject] = annotationsPos
    .map(anno => PosAnnotationJsonProtocol.PosAnnotationJsonFormat.write(anno))
    .toVector

  override def toString: String = Strings.analysedArticleString(_id, longUrl, crawlTime.toString, text, annotationsPos)
}

//TODO update if needed else delete
object AnalysedArticleJsonProtocol extends DefaultJsonProtocol{

  implicit object AnnotatedArticleJsonFormat extends RootJsonFormat[AnnotatedArticle] {

    override def read(json: JsValue): AnnotatedArticle = ???

    override def write(annotatedArticle: AnnotatedArticle): JsObject = JsObject(
        "_id" -> JsString(annotatedArticle._id),
        "longUrl" -> JsString(annotatedArticle.longUrl),
        "crawlTime" -> JsString(annotatedArticle.crawlTime.toString),
        Strings.columnText -> JsString(annotatedArticle.text),
        "annotationsPos" -> JsArray(annotatedArticle.annotationsPosAsJsObject)
        //"tagsPercentage" -> JsObject(annotatedArticle.tagPercentage.toMap.mapValues(percentage => JsNumber(percentage)))
      )
  }
}


