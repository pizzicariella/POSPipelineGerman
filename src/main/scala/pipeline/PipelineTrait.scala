package pipeline

import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.DataFrame

trait PipelineTrait {
  /**
   * Trains the pipeline on given articles, saves model if path is given and transforms given articles according to
   * model.
   * @param articles: A Dataframe with column 'text', e.g. (id, long_url, crawl_time, text) for News articles.
   * @param path: Destination to save model. If None (default) is given, model won't be saved.
   * @return The transformed DataFrame
   */
  def runPipeline(articles: DataFrame, path: Option[String] = None): DataFrame

  /**
   * Trains Pipeline on given DataFrame and saves model if path is given.
   * @param articles: A Dataframe with column 'text', e.g. (id, long_url, crawl_time, text) for News articles.
   * @param path: Destination to save model. If None (default) is given, model won't be saved.
   * @return The trained PipelineModel
   */
  def train(articles: DataFrame, path: Option[String] = None): PipelineModel

  /**
   * Transforms articles using previously trained PipelineModel.
   * @param articles: A Dataframe with column 'text', e.g. (id, long_url, crawl_time, text) for News articles.
   * @param path: The path to load the PipelineModel
   * @return The transformed DataFrame
   */
  def annotate(articles: DataFrame, path: String): DataFrame
}
