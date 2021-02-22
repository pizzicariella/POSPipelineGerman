package evaluation

import org.apache.spark.sql.DataFrame

trait Evaluator {

  /**
   * Evaluates Model by calculating.
   * @param testArticles - DataFrame containing articles, transformed on model that should be tested
   * @param goldStandard - DataFrame with correct annotations, ids have to match with ids of testArticles
   */
  def evaluateModel(testArticles: DataFrame, goldStandard: DataFrame): DataFrame
}
