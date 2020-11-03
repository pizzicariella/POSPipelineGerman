package evaluation

class AnnotatorEvaluator extends Evaluator {

  override def getAccuracy(annotated: List[List[String]], correct: List[List[String]]): Double = {
    val annosTotal = annotated.map(list => list.size).reduce(_+_)
    val annosCorrect = annotated.zip(correct)
      .map(lists => lists._1.zip(lists._2)
      .map(anno => anno._1 match {
        case x if x.equals(anno._2) => 1
        case _ => 0
      })
      .reduce(_+_))
      .reduce(_+_)

    annosCorrect.toDouble / annosTotal
  }
}
