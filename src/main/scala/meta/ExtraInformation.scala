package meta

import model.PosAnnotation

object ExtraInformation {

  /*def getPosPercentage(annotatedRdd: RDD[(String, String, String, String, List[PosAnnotation])]):
          RDD[(String, String, String, String, List[PosAnnotation], Map[String, Double])] = {

    annotatedRdd.map(attribs => (attribs._1, attribs._2, attribs._3, attribs._4, attribs._5, attribs._5.))
  } */

  def getPosPercentage(annos: Seq[PosAnnotation]): List[(String, Double)] = {
    val annosTotal = annos.size
    annos
      .foldLeft(Map.empty[String, Double])((map, anno) => map.updated(anno.tag, map.getOrElse(anno.tag, 0.0)+1.0))
      .mapValues(_/annosTotal)
      .toList
  }

}
