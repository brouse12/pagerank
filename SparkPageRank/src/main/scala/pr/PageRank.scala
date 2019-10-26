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

    // Convert vertex pairs to an adjacency list
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

    // Add dangling pages and cache the final graph for future use.
    val graphRDD = graphRDDWithNoDPages.union(danglingPagesRDD)
      .cache()


    // Assign an initial rank to each vertex and create dummy vertex 0

    var ranks = graphRDD.mapValues(v => 1.0)
      .union(sc.parallelize(Seq(("0", 0.0))).partitionBy(graphPartitioner))

    //    val test = graphRDD.map(value => value)
    //    println("Graph partitioner is: " + graphRDD.partitioner.toString)
    //    println("Dangling partitioner is: " + danglingPagesRDD.partitioner.toString)
    //    println("Test partitioner is: " + test.partitioner.toString)
    //    println("Ranks partitioner is: " + ranks.partitioner.toString)
    //graphRDD.foreach(tup => println(tup._1 + ": " + tup._2.toList.toString()))


    //TODO: no one's pointing to 1,6,11,16,21 - should they still get the 0.15? We can add that processing at the end!
    for (i <- 1 to args(1).toInt) {
      val contributions = graphRDD.join(ranks).values.flatMap { case (vertices, rank) =>
        val size = vertices.size
        vertices.map(vertex => (vertex, rank / size))
      }
      ranks = contributions.reduceByKey(graphPartitioner, _ + _).mapValues(0.15 + 0.85 * _)
      println("Ranks partitioner is: " + ranks.partitioner.toString)
    }

    //val output = ranks.collect()
    //output.foreach(tup => println(s"${tup._1} has rank:  ${tup._2} ."))
    //ranks.saveAsTextFile(args(3))
    sc.stop()
  }


}