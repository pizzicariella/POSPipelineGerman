package evaluation

import org.apache.spark.sql.DataFrame

trait Evaluator {

  /**
   * Evaluates Model by calculating accuracy after transforming testArticles by using given model.
   * @param testArticles - DataFrame containing raw articles, not transformed yet
   * @param goldStandard - DataFrame with correct annotations, ids have to match with ids of testArticles
   * @param pathToModel
   */
  def evaluateModel(testArticles: DataFrame, goldStandard: DataFrame, pathToModel: String): DataFrame
}
