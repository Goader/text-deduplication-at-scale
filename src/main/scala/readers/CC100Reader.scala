package ua.nlp.ukrlm
package readers

import models.Document

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object CC100Reader {
  def apply(spark: SparkSession, path: String): RDD[Document] = {
    val lines = spark.sparkContext.textFile(path)

    val rdd = lines
      .filter(_.nonEmpty)
      .zipWithIndex
      .map { case (line, idx) =>
        val id = idx
        val text = line
        Document(id, text)
      }

    rdd
  }
}
