package pipeline

import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.DataFrame

trait PipelineTrait {

  /**
   * Trains and runs the pipeline
   * @param articles A sequence of articles with metadata and text, e.g. (id, long_url, crawl_time, full_text)
   * @param replacements A map with patterns for regex replacements, e.g. Map(" " -> " ", "(?<=[^A-Z\\d])\\b\\.\\b" -> ". ")
   * @return
   */
  def runPipeline(articles: Seq[(String, String, String, String)],
                  replacements: Map[String, String]): DataFrame

  /**
   * Trains Pipeline with given dataset.
   * @param articles
   * @param replacements replacement patterns to prepare dataset
   * @return
   */
  def train(articles: Seq[(String, String, String, String)],
            replacements: Map[String, String]): PipelineModel

  /**
   * Annotate articles using previously trained PipelineModel.
   * @param articles
   * @param replacements
   * @param path
   * @return
   */
  def annotate(articles: Seq[(String, String, String, String)],
              replacements: Map[String, String],
              path: String): DataFrame
}
