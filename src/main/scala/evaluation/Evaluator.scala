package evaluation

import org.apache.spark.sql.DataFrame

trait Evaluator {

  /**
   * Evaluates Model by calculating accuracy.
   * @param testArticles DataFrame containing articles, transformed on model that should be evaluated
   * @param goldStandard DataFrame with correct annotations, ids have to match ids of testArticles
   * @return DataFrame containing accuracy column
   */
  def evaluateModel(testArticles: DataFrame, goldStandard: DataFrame): DataFrame

  /**
   * Evaluates Model by calculating recall for given tags.
   * @param testArticles - DataFrame containing articles, transformed on model that should be evaluated
   * @param goldStandard - DataFrame with correct annotations, ids have to match ids of testArticles
   * @param tagList - Tags to evaluate
   * @return DataFrame containing recall column
   */
  def evaluationForTags(testArticles: DataFrame, goldStandard: DataFrame, tagList: List[String]): DataFrame
}
