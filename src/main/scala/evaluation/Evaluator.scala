package evaluation

import org.apache.spark.sql.DataFrame

trait Evaluator {

  /**
   * Evaluates Model by calculating accuracy.
   * @param testArticles - DataFrame containing articles, transformed on model that should be tested
   * @param goldStandard - DataFrame with correct annotations, ids have to match with ids of testArticles
   */
  def evaluateModel(testArticles: DataFrame, goldStandard: DataFrame): DataFrame

  /**
   * Evaluates Model by calculating accuracy for given tags.
   * @param testArticles - DataFrame containing articles, transformed on model that should be tested
   * @param goldStandard - DataFrame with correct annotations, ids have to match with ids of testArticles
   * @param tagList - Tags to evaluate
   * @return
   */
  def evaluationForTags(testArticles: DataFrame, goldStandard: DataFrame, tagList: List[String]): DataFrame
}
