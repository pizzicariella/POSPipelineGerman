package pipeline

import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.DataFrame

trait PipelineTrait {

  /**
   * Trains and runs the pipeline
   * @param articles A Dataframe with column 'text', e.g. (id, long_url, crawl_time, text) for News article
   * @return
   */
  def runPipeline(articles: DataFrame): DataFrame

  /**
   * Trains Pipeline with given dataset.
   * @param articles must contain column text
   * @return
   */
  def train(articles: DataFrame, path: Option[String] = None): PipelineModel

  /**
   * Annotate articles using previously trained PipelineModel.
   * @param articles must contain column text
   * @param path
   * @return
   */
  def annotate(articles: DataFrame, path: String): DataFrame
}
