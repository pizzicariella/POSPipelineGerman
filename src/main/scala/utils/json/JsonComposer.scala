package utils.json

import model.{AnalysedArticle, AnalysedArticleJsonProtocol}
import spray.json._

object JsonComposer {

  def composeAnalysedArticleJson(article: AnalysedArticle): String =
    article
      .toJson(AnalysedArticleJsonProtocol.AnalysedArticleJsonFormat)
      .compactPrint

}
