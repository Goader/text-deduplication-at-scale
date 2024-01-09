package ua.nlp.ukrlm
package deduplication

import org.apache.spark.rdd.RDD

import scala.util.Random


object MinhashLSH {
  val MaxHash = 4294967295L
  val MersennePrime = 2305843009213693951L

  def apply(
    contentCol: String,
    idCol: String,
    numPerm: Int,
    ngramSize: Int,
    bandsCount: Int,
    rowsCount: Int,
    minNgramSize: Int,
    seed: Int = 0,
  ): MinhashLSH = {

    // computing permutations
    val rand = new Random(seed)
    val permutations = (0 until numPerm).map(_ => {
      val a = rand.nextLong(MersennePrime - 1) + 1
      val b = rand.nextLong(MersennePrime)
      (a, b)
    })

    // computing hash ranges
    val hashRanges = (0 until bandsCount).map(bandIdx => {
      val start = bandIdx * rowsCount
      val end = (bandIdx + 1) * rowsCount - 1
      (start, end)
    })

    new MinhashLSH(
      contentCol,
      idCol,
      permutations,
      ngramSize,
      hashRanges,
      minNgramSize
    )
  }
}

class MinhashLSH(
  contentCol: String,
  idCol: String,
  permutations: Seq[(Long, Long)],
  ngramSize: Int,
  hashRanges: Seq[(Int, Int)],
  minNgramSize: Int
) extends java.io.Serializable {

  private def sha1_hash32(payload: String): Int = {
    val md = java.security.MessageDigest.getInstance("SHA-1")
    val digest = md.digest(payload.getBytes("UTF-8"))
    val hash = java.nio.ByteBuffer.wrap(digest).getInt
    hash
  }

  private def produceHashBands(
    content: String
  ): Seq[Array[Byte]] = {
    val tokens = content.split("\\W+")

    val minHashes = if (tokens.length < minNgramSize) {
      // MAX_HASH is used as a placeholder for empty content (permutations.length times)
      Seq.fill(permutations.length)(MinhashLSH.MaxHash.toInt)
    } else {
      val ngrams = tokens.sliding(ngramSize, 1).map(_.mkString(" ")).toSeq
      val hashes = ngrams.map(sha1_hash32)

      permutations.map(permutation => {
        val (a, b) = permutation
        val minHash = hashes.map(hash => {
          (hash * a + b) % MinhashLSH.MersennePrime
        }).min
        (minHash & MinhashLSH.MaxHash).toInt
      })
    }

    val bands = hashRanges.map(hashRange => {
      val (start, end) = hashRange
      val band = minHashes.slice(start, end + 1)

      val byteBuffer = java.nio.ByteBuffer.allocate(band.length * 4)
      byteBuffer.order(java.nio.ByteOrder.LITTLE_ENDIAN)
      band.foreach(byteBuffer.putInt)

      byteBuffer.array
    })

    bands
  }

  // receives RDD of dictionaries with keys: idCol, contentCol
  // returns RDD of dictionaries with keys: "bandIdx", "band", idCol
  def run(
    rdd: RDD[Map[String, String]],
  ): RDD[Map[String, Any]] = {
    val rddWithBands = rdd.flatMap(row => {
      val content = row(contentCol)
      val id = row(idCol)
      val bands = produceHashBands(content)
      val bandsWithIdx = bands.zipWithIndex
      val bandsWithIdxWithId = bandsWithIdx.map(bandWithIdx => {
        val band = bandWithIdx._1
        val idx = bandWithIdx._2
        Map(
          "bandIdx" -> idx,
          "band" -> band,
          idCol -> id,
        )
      })
      bandsWithIdxWithId
    })

    rddWithBands
  }
}
