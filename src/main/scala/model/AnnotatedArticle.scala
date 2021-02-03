package model

import java.sql.Timestamp


case class AnnotatedArticle(_id: String,
                            longUrl: String,
                            crawlTime: Timestamp,
                            text: String,
                            annotationsPos: List[PosAnnotation],
                            lemmas: List[Lemma],
                            tagPercentage: List[PosPercentage]) {
}


