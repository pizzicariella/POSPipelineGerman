package daos.memory

import java.io.File
import daos.DAO
import model.AnnotatedArticle
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.reflect.io.Directory


class InMemoryDao(val spark: SparkSession) extends DAO{

  override def getNewsArticles(limit: Option[Int], file: String): DataFrame = {

    val articles = spark.read.json(file)
      .drop("short_url",
        "keywords",
        "published_time",
        "news_site",
        "image_links",
        "description",
        "authors",
        "links")

    limit match {
      case Some(x) => articles.limit(x)
      case None => articles
    }
  }


  override def writeArticle(article: AnnotatedArticle, destination: String): Unit = ???

  /**
   * Writes multiple analysed articles to destination
   *
   * @param articles
   * @param destination
   */

  override def writeArticles(articles: DataFrame, destination: String): Unit = {
      new Directory(new File(destination)).deleteRecursively()
      articles.write.json(destination)
  }
}

