package pipeline

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
}
