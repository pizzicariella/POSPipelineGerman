package daos.memory

import java.io.File
import java.nio.file.{Files, Paths}

import daos.DAO
import model.{AnnotatedArticle, NewsArticle}
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.FileIO
import utils.json.JsonComposer
import utils.json.JsonParser.parseNewsArticle

import scala.reflect.io.Directory


class InMemoryDao(val spark: SparkSession) extends DAO{

  override def getNewsArticles(limit: Option[Int], file: String): DataFrame = {//Seq[NewsArticle] = {

    //val articles = FileIO.readJsonFile(file)
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

    /*val until = limit match {
      case Some(x) => x
      case None => articles.size
    }

    val sliced = articles.slice(0, until)
    sliced.map(doc => parseNewsArticle(doc))*/

  }


  override def writeArticle(article: AnnotatedArticle, destination: String): Unit = ???

  /**
   * Writes multiple analysed articles to destination
   *
   * @param articles
   * @param destination
   */
    //TODO test
  override def writeArticles(articles: DataFrame, destination: String): Unit = {
    //val jsonList = articles.map(article => JsonComposer.composeAnalysedArticleJson(article))
    //FileIO.writeJsonFile(destination, jsonList)
      new Directory(new File(destination)).deleteRecursively()
      //articles.coalesce(1).write.format("json").save(destination)
      articles.write.json(destination)
      //articles.show(false)
  }
}

