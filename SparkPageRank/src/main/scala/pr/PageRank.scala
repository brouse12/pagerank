package pr

import org.apache.log4j.LogManager
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
    if (args.length != 4) {
      logger.error("Usage: <k> <iterations> <input dir> <output dir>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Page Rank")
    val sc = new SparkContext(conf)

    //TODO: replace lines w/ auto generated graph
    //TODO: w/ linear chains, nobody has extra vertices pointing to them? They'll be equally important?
    val lines = sc.textFile(args(2))
      .map(line => line.split(","))

    //TODO: find better way of adding dangling pages to graph?

    // Convert vertex pairs to an adjacency list, without checking for dangling pages
    val graphRDDWithNoDPages = lines.map(edgeArray => (edgeArray(0), edgeArray(1)))
      .distinct()
      .groupByKey()

    // Find the adjacency list's partitioner
    val graphPartitioner = graphRDDWithNoDPages.partitioner match {
      case Some(p) => p
      case (None) => new HashPartitioner(graphRDDWithNoDPages.partitions.length)
    }

    // Identify dangling pages.  Use the graph partitioner to facilitate shuffle-free union.
    val danglingPagesRDD = lines.map(edgeArray => (edgeArray(1), "0"))
      .distinct()
      .groupByKey(graphPartitioner)
      .subtractByKey(graphRDDWithNoDPages)

    // Add dangling pages and persist the final graph for future use.
    val graphRDD = graphRDDWithNoDPages.union(danglingPagesRDD)
      .persist()

    // Assign an initial rank to each vertex and create dummy vertex 0
    val k = args(0).toDouble
    val initialRank: Double = 1.0 / (k * k)
    var ranks = graphRDD.mapValues(v => initialRank)
      .union(sc.parallelize(Seq(("0", 0.0))).partitionBy(graphPartitioner))

    //    val test = graphRDD.map(value => value)
    //    println("Graph partitioner is: " + graphRDD.partitioner.toString)
    //    println("Dangling partitioner is: " + danglingPagesRDD.partitioner.toString)
    //    println("Test partitioner is: " + test.partitioner.toString)
    //    println("Ranks partitioner is: " + ranks.partitioner.toString)
    //graphRDD.foreach(tup => println(tup._1 + ": " + tup._2.toList.toString()))

    for (i <- 1 to args(1).toInt) {
      val contributions = graphRDD.join(ranks).flatMap { case (vertex, (links, rank)) =>
        // Each vertex receives a 0.0 contribution, to ensure that vertices with no backlinks get a rank
        val zeroContribution = Iterable[(String, Double)]((vertex, 0.0))
        val size = links.size
        links.map(destination => (destination, rank / size)) ++ zeroContribution
      }
        .reduceByKey(graphPartitioner, _ + _)

      val danglingMass = contributions.lookup("0").head
      ranks = contributions.mapValues(v => v + danglingMass / (k * k))
    }

    val output = ranks.collect()
    output.foreach(tup => println(s"${tup._1} has rank:  ${tup._2} ."))

    val total = ranks.map({ case (k, v) => if (k.equals("0")) (k, 0.0) else (k, v) }).values.sum
    println("Total ranks = " + total + "\n")

    sc.stop()
  }


}