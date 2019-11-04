package pr

import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * Spark Scala program to run the page rank algorithm in a distributed environment.
 * Handles dangling pages via a dummy node approach.  Runs a specified number of iterations rather
 * than checking for convergence.
 */
object PageRank {
  val LinkProbability = 0.85 // alpha probability for linking to a page.
  val JumpProbability = 0.15 // 1 - alpha probability for jumping to a page.

  /**
   * Saves final page ranks to user-specified output directory after user-specified number of
   * iterations.  The input graph is generated - k chains of k nodes to guarantee dangling pages and
   * pages with no inlinks.
   *
   * @param args k numIterations outputDirectory
   */
  def main(args: Array[String]) {
    val logger = LogManager.getRootLogger

    if (args.length != 3) {
      logger.error("Usage: <k> <iterations> <output dir>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Page Rank")
    val sc = new SparkContext(conf)

    // Set up some values
    val k = args(0).toInt
    val numVertices: Long = k * k
    val probJumpToPage = sc.broadcast[Double](JumpProbability * (1.0 / numVertices))

    // Generate an edge set graph per to user input size, then partition it as an adjacency list
    // Generated graph includes dangling page links to dummy vertex.
    val graphRDD = generateGraph(k, sc).groupByKey().persist();

    // Find the adjacency list's partitioner.
    val graphPartitioner = graphRDD.partitioner match {
      case Some(p) => p
      case (None) => new HashPartitioner(graphRDD.partitions.length)
    }

    // Assign an initial rank to each vertex and create dummy vertex 0.
    val initialRank: Double = 1.0 / numVertices
    val dummyVertex = sc.parallelize(Seq((0, 0.0))).partitionBy(graphPartitioner)
    var ranksRDD = graphRDD.mapValues(v => initialRank).union(dummyVertex)

    // Debugging statements for checking partitioners.
    logger.info("Graph partitioner is: " + graphRDD.partitioner.toString)
    logger.info("Ranks partitioner is: " + ranksRDD.partitioner.toString)

    // Run PageRank for as many iterations as specified by user.
    // Vertices that receive no contributions via inlinks are caught with a leftOuterJoin and assigned a rank.
    var danglingMass = 0.0
    for (i <- 0 until args(1).toInt) {
      logger.info("Testing intermediate rank caching. Iteration number: " + i)
      val rankWithNoInlink = probJumpToPage.value + LinkProbability * danglingMass

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
      ranksRDD = contributions.mapValues(v => probJumpToPage.value + LinkProbability * (v + danglingMass))
    }

    logger.info(ranksRDD.toDebugString)

    // Add in vertices that receive no contributions via inlinks.
    val rankWithNoInlink = probJumpToPage.value + LinkProbability * danglingMass
    val missingNodes = graphRDD.subtractByKey(ranksRDD).mapValues(v => rankWithNoInlink)
    ranksRDD = ranksRDD.union(missingNodes)

    // Output results.
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