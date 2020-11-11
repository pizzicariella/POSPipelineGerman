package evaluation

trait Evaluator {
  /**
   * Calculates accuracy of annotations. Pass lists of documents consisting of a list with annotations.
   * @param annotated actual annotations
   * @param correct correct annotations
   */
  def getAccuracy(annotated: List[List[String]], correct: List[List[String]]): Double

  /**
   * Allows to compare results of pipeline with correct annotations
   * @param annotated
   * @param correct
   * @return
   */
  def compare(annotated: List[List[String]], correct: List[List[String]]): List[List[(String, String)]]
}
