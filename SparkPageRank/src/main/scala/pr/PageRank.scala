package pr

import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 *
 */
object PageRank {

  /**
   * aa
   *
   * @param args aa
   */
  def main(args: Array[String]) {
    val logger = LogManager.getRootLogger

    if (args.length != 3) {
      logger.error("Usage: <k> <iterations> <output dir>")
      System.exit(1)
    }

    //TODO: w/ linear chains, nobody has extra vertices pointing to them? They'll be equally important?
    val conf = new SparkConf().setAppName("Page Rank")
    val sc = new SparkContext(conf)

    // Set up some values
    val probLink = 0.85 // alpha probability for linking to a page
    val probJump = 0.15 // 1 - alpha probabilty for jumping to a page
    val k = args(0).toInt
    val numVertices: Long = k * k
    val probJumpToN = sc.broadcast[Double](probJump * (1.0 / numVertices)) // Probability of jumping to a particular vertex

    // Generate an edge set graph per to user input size, then partition it as an adjacency list
    // Generated graph includes dangling page links to dummy vertex
    val graphRDD = generateGraph(k, sc).groupByKey().persist()

    // Find the adjacency list's partitioner
    val graphPartitioner = graphRDD.partitioner match {
      case Some(p) => p
      case (None) => new HashPartitioner(graphRDD.partitions.length)
    }

    // Assign an initial rank to each vertex and create dummy vertex 0
    val initialRank: Double = 1.0 / numVertices
    val dummyVertex = sc.parallelize(Seq((0, 0.0))).partitionBy(graphPartitioner)
    var ranksRDD = graphRDD.mapValues(v => initialRank).union(dummyVertex)

    // Debugging statements for checking partitioners
    logger.info("Graph partitioner is: " + graphRDD.partitioner.toString)
    logger.info("Ranks partitioner is: " + ranksRDD.partitioner.toString)

    // Run PageRank for as many iterations as specified by user
    // Vertices that receive no contributions via inlinks are caught with a leftOuterJoin and assigned a rank
    var danglingMass = 0.0
    for (i <- 0 until args(1).toInt) {
      logger.info("Testing intermediate rank caching. Iteration number: " + i)
      val rankWithNoInlink = probJumpToN.value + probLink * danglingMass

      val contributions = graphRDD.leftOuterJoin(ranksRDD)
        .flatMap { case (vertex, (links, optionalRank)) =>
          val rank = optionalRank match {
            case Some(r) => r
            case None => rankWithNoInlink
          }
          val size = links.size
          links.map(destination => (destination, rank / size))
        }
        .reduceByKey(graphPartitioner, _ + _)

      danglingMass = contributions.lookup(0).head / numVertices
      //TODO: test speedup for using/not using cache/persist, for graphRDD and RanksRDD.
      ranksRDD = contributions.mapValues(v => probJumpToN.value + probLink * (v + danglingMass))
    }

    logger.info(ranksRDD.toDebugString)

    // Add in vertices that receive no contributions via inlinks
    val rankWithNoInlink = probJumpToN.value + probLink * danglingMass
    val missingNodes = graphRDD.subtractByKey(ranksRDD).mapValues(v => rankWithNoInlink)
    ranksRDD = ranksRDD.union(missingNodes)

    // Output results
    ranksRDD.saveAsTextFile(args(2))
    val total = ranksRDD.filter(vertexRank => vertexRank._1 != 0).values.sum
    logger.info("Total ranks after " + args(1).toInt + " iterations: " + total)
  }

  /**
   * Generates an edge set graph consisting of k linear chains with k vertices. Dangling pages are
   * linked to dummy node 0.
   *
   * @param k  the graph size parameter
   * @param sc the SparkContext
   * @return a (vertex, outlink) RDD
   */
  def generateGraph(k: Int, sc: SparkContext): RDD[(Int, Int)] = {
    var counter = 0
    var edgeSet = Seq[(Int, Int)]()

    for (i <- 0 until k) {
      counter += 1
      for (j <- 0 until (k - 1)) {
        edgeSet = edgeSet :+ (counter, counter + 1)
        counter += 1
      }
      edgeSet = edgeSet :+ (counter, 0)
    }
    sc.parallelize(edgeSet)
  }


}