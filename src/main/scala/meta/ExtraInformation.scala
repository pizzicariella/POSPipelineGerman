package meta

import model.PosAnnotation

object ExtraInformation {

  def getPosPercentage(annos: Seq[PosAnnotation]): List[(String, Double)] = {
    val annosTotal = annos.size
    annos
      .foldLeft(Map.empty[String, Double])((map, anno) => map.updated(anno.tag, map.getOrElse(anno.tag, 0.0)+1.0))
      .mapValues(_/annosTotal)
      .toList
  }
}
