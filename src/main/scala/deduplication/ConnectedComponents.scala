package ua.nlp.ukrlm
package deduplication

import org.apache.spark.rdd.RDD

import scala.annotation.tailrec

object ConnectedComponents extends Serializable {
  def generateEdges(nodeIds: Seq[Long]): Seq[(Long, Long)] = {
    if (nodeIds.length < 2) {
      Seq.empty
    } else {
      val minNodeId = nodeIds.min

      val edges = nodeIds
        .filter(_ != minNodeId)
        .map(nodeId => (minNodeId, nodeId))

      edges
    }
  }

  private def smallStar(edges: RDD[(Long, Long)]): RDD[(Long, Long)] = {
    edges
      .map { case (node1, node2) =>
        if (node1 <= node2) {
          (node2, node1)
        } else {
          (node1, node2)
        }
      }
      .groupByKey()
      .flatMap { case (node, neighbors) =>
        val allNodes = node +: neighbors.toSeq
        val minNode = allNodes.min

        val edges = allNodes
          .filter(_ != minNode)
          .map(node => (node, minNode))

        edges
      }
  }

  private def largeStar(edges: RDD[(Long, Long)]): RDD[(Long, Long)] = {
    edges
      .map(edge => (edge._2, edge._1))
      .union(edges)
      .groupByKey()
      .flatMap { case (node, neighbors) =>
        val allNodes = node +: neighbors.toSeq
        val minNode = allNodes.min

        val edges = allNodes
          .filter(_ > node)
          .map(node => (node, minNode))

        edges
      }
  }

  @tailrec
  private def alternatingAlgorithm(
    edges: RDD[(Long, Long)],
    maxIterations: Int,
    iteration: Int = 0,
    converged: Boolean = false
  ): (RDD[(Long, Long)], Boolean, Int) = {

    println("iteration: " + iteration)
    if (iteration >= maxIterations || converged) {
      println("converged: " + converged)
//      println("edges: " + edges.count())
      (edges, converged, iteration)
    } else {
      val largeStarEdges = largeStar(edges).distinct().cache()
      val smallStarEdges = smallStar(largeStarEdges).distinct().cache()

//      println("largeStarEdges: " + largeStarEdges.count())
//      println("smallStarEdges: " + smallStarEdges.count())

      val largeStarChanges = largeStarEdges.subtract(edges)
      val smallStarChanges = smallStarEdges.subtract(largeStarEdges)

      val newConverged = largeStarChanges.isEmpty() && smallStarChanges.isEmpty()

      alternatingAlgorithm(smallStarEdges, maxIterations, iteration + 1, newConverged)
    }
  }

  def run(
    edges: RDD[(Long, Long)],
    maxIterations: Int = 10
  ): RDD[(Long, Long)] = {
    val (finalEdges, _, _) = alternatingAlgorithm(edges, maxIterations)
    finalEdges
  }
}
