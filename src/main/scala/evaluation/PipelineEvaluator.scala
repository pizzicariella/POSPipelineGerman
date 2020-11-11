package evaluation

//TODO refactor
class PipelineEvaluator extends Evaluator {

  //TODO should this method work with spark sql? (df)
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

  override def compare(annotated: List[List[String]], correct: List[List[String]]): List[List[(String, String)]] = {
    if (annotated.size != correct.size) {
      println("Number of analyzed documents does not match.")
    }

    val combined = annotated.zip(correct)

    combined
      .map(lists => equalListSize(lists._1, lists._2))
      .zipWithIndex
      .foreach(x => x._1 match {
        case true => println("Document "+x._2+": Number of annotations is equal")
        case false => println("Document "+x._2+": Number of annotations is not equal")
    })

    combined.map(lists => lists._1.zip(lists._2))
  }

  def equalListSize(l1: List[String], l2: List[String]): Boolean = {
    if(l1.size == l2.size){
      true
    } else false
  }

}
