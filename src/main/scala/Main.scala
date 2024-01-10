package ua.nlp.ukrlm

import readers.CC100Reader
import deduplication.{ConnectedComponents, MinhashLSH}

object Main {
  def main(args: Array[String]): Unit = {
    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local[*]")
      .appName("text-deduplication")
      .getOrCreate

    spark.sparkContext.setLogLevel("WARN")

    val cc100 = CC100Reader(spark, "/media/goader/masters/corpora/cc-100/uk-10m.txt")
    cc100.take(10).foreach(println)

    val minhashLSH = MinhashLSH(
      numPerm = 32,
      ngramSize = 3,
      bandsCount = 10,
      rowsCount = 3,
      minNgramSize = 3,
      seed = 0
    )

    val cc100Bands = minhashLSH.run(cc100)
    cc100Bands.take(30).foreach(docBand => {
      println("docId: " + docBand.docId + ", bandIdx: " + docBand.bandIdx)
      println(docBand.band.bytes.mkString("Array(", ", ", ")") + "\n")
    })

    // groupoing by bandIdx and band, which is an array of bytes
    val edges = cc100Bands
      .groupBy(docBand => (docBand.bandIdx, docBand.band))
      .flatMap { case (_, docBands) =>
        val docIds = docBands.map(_.docId).toSeq
        ConnectedComponents.generateEdges(docIds)
      }
      .distinct()
      .cache()

    edges.take(10).foreach(edge => {
      println("edge: " + edge)
      println("doc1: " + cc100.filter(_.id == edge._1).take(1).head)
      println("doc2: " + cc100.filter(_.id == edge._2).take(1).head)
      println()
    })

    val connectedComponents = ConnectedComponents.run(edges)
    connectedComponents.take(10).foreach(println)

    connectedComponents.map(x => {
        if (x._1 < x._2) {
          (x._1, x._2)
        } else {
          (x._2, x._1)
        }
      })
      .groupByKey()
      .take(10)
      .foreach(x => {
        println("component: " + x._1)
        println("doc: " + cc100.filter(_.id == x._1).take(1).head)

        println("docs:")
        x._2.foreach(docId => {
          println("doc: " + cc100.filter(_.id == docId).take(1).head)
        })
        println()
      })

    connectedComponents.collect()

    // TODO: persist
  }
}