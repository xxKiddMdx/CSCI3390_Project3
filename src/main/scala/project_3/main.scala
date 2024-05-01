package project_3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}

object main {
  // Set logging levels to reduce console noise
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)

  // Define a case class to store vertex properties
  case class VertexProperties(vertexId: Long, vertexDegree: Int, randomValue: Double, status: String, isInMIS: String)

  // Function to check if any vertices are still 'active'
  def isAnyVertexActive(graph: Graph[VertexProperties, Int]): Boolean = {
    graph.vertices.filter{ case (_, properties) => properties.status == "active"}.count() > 0
  }

  // Function to check if a specific value exists in an RDD
  def isValueInRDD(rdd: RDD[Long], targetValue: Long): Boolean = {
    rdd.filter(_ == targetValue).count() > 0
  }

  // Implementation of Luby's Maximal Independent Set (MIS) algorithm
  def LubyMIS(graphInput: Graph[Int, Int]): Graph[Int, Int] = {
    var selectedVertices = Set[Long]()
    val vertexDegrees = graphInput.degrees
    var graphWithDegrees = graphInput.outerJoinVertices(vertexDegrees)({ case (_, _, optDegree) => optDegree.getOrElse(0)})
    var modifiedGraph = graphWithDegrees.mapVertices((id, degree) => VertexProperties(id, degree, -0.1, "active", "No"))
    modifiedGraph.cache() // Cache the graph for performance

    var numIterations = 0

    // Iterate until no active vertices remain
    while(isAnyVertexActive(modifiedGraph)){
      modifiedGraph = modifiedGraph.mapVertices((_, properties: VertexProperties) => {
        if (properties.status != "active") {
          properties
        } else {
          val rand = new scala.util.Random
          VertexProperties(properties.vertexId, properties.vertexDegree, rand.nextDouble(), properties.status, properties.isInMIS)
        }
      })

      // Aggregate messages to find the vertex with the highest random value in each neighborhood
      val highestNeighborValues: VertexRDD[VertexProperties] = modifiedGraph.aggregateMessages[VertexProperties](
        triplet => {
          triplet.sendToDst(triplet.srcAttr)
          triplet.sendToSrc(triplet.dstAttr)
        },
        (firstProp, secondProp) => if (firstProp.randomValue > secondProp.randomValue) firstProp else secondProp
      )

      // Filter out the vertices that are superior in their neighborhood
      val joinedVertexProperties = modifiedGraph.vertices.join(highestNeighborValues).filter({
        case (_, (properties, competing)) => properties.status == "active"
      })

      val superiorVertexProperties = joinedVertexProperties.filter({
        case (_, (properties, competing)) => properties.randomValue > competing.randomValue
      })

      // Update the graph based on the vertices selected in this iteration
      val vertexIdsSelected = superiorVertexProperties.map({ case ((id, (properties, _))) => properties.vertexId}).distinct().collect().toSet
      val activeInMISGraph = graphInput.mapVertices((id, _) => vertexIdsSelected.contains(id))
      val neighborsInMIS: VertexRDD[Boolean] = activeInMISGraph.aggregateMessages[Boolean](
        triplet => {
          triplet.sendToDst(triplet.srcAttr)
          triplet.sendToSrc(triplet.dstAttr)
        },
        _ || _
      )
      val neighborVertices = neighborsInMIS.filter({ case (_, isInMIS) => isInMIS}).map({ case (id, _) => id}).distinct().collect().toSet

      // Deactivate vertices and update their MIS status
      modifiedGraph = modifiedGraph.mapVertices((id, properties) => {
        if (vertexIdsSelected.contains(id)) {
          VertexProperties(properties.vertexId, properties.vertexDegree, -0.1, "inactive", "Yes")
        } else if (neighborVertices.contains(id)) {
          VertexProperties(properties.vertexId, properties.vertexDegree, -0.1, "inactive", "No")
        } else {
          properties
        }
      })
      println("\tNumber of vertices remaining: " + modifiedGraph.vertices.filter({ case (_, properties) => properties.status == "active"}).count())
      numIterations += 1
    }
    println("Number of iterations: " + numIterations)
    val vertexRDD = modifiedGraph.vertices

    // Collect vertices that are in the MIS
    val verticesInMIS: RDD[VertexId] = vertexRDD
      .filter{ case (_, properties) => properties.isInMIS == "Yes"}
      .map { case (id, _) => id}

    // Final output graph where vertices in MIS are marked as '1', others as '-1'
    val vertexIdsInMIS: Array[VertexId] = verticesInMIS.collect()
    val outputGraph = graphInput.mapVertices((id, _) => if (vertexIdsInMIS.contains(id)) 1 else -1)
    return outputGraph
  }

  // Helper function to check if a neighbor with a specific attribute exists
  def hasNeighborWithAttributeOne(triplet: EdgeContext[Int, Int, Boolean], direction: String): Boolean = {
    if (triplet.srcAttr == 1 && direction == "to_dst") {
      return true
    } else if (triplet.srcAttr == -1 && direction == "to_dst") {
      return false
    } else if (triplet.dstAttr == 1 && direction == "to_src") {
      return true
    } else {
      return false
    }
  }


def verifyMIS(g: Graph[Int, Int]): Boolean = {
  val independent = g.triplets.flatMap { triplet =>
    if (triplet.srcAttr == 1 && triplet.dstAttr == 1) {
      Some((triplet.srcId, triplet.dstId))
    } else None
  }.count() == 0

  val maximal = g.vertices.leftOuterJoin(g.aggregateMessages[Int](
    triplet => {
      if (triplet.srcAttr == 1) triplet.sendToDst(1)
      if (triplet.dstAttr == 1) triplet sendToSrc(1)
    },
    (a, b) => a + b
  )).map {
    case (id, (label, Some(count))) => label == 1 || count > 0
    case (id, (label, None)) => label == 1
  }.reduce(_ && _)

  independent && maximal
}

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("project_3")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()
/* You can either use sc or spark */

    if(args.length == 0) {
      println("Usage: project_3 option = {compute, verify}")
      sys.exit(1)
    }
    if(args(0)=="compute") {
      if(args.length != 3) {
        println("Usage: project_3 compute graph_path output_path")
        sys.exit(1)
      }
      val startTimeMillis = System.currentTimeMillis()
      val edges = sc.textFile(args(1)).map(line => {val x = line.split(","); Edge(x(0).toLong, x(1).toLong , 1)} )
      val g = Graph.fromEdges[Int, Int](edges, 0, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)
      val g2 = LubyMIS(g)

      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
      println("==================================")
      println("Luby's algorithm completed in " + durationSeconds + "s.")
      println("==================================")

      val g2df = spark.createDataFrame(g2.vertices)
      g2df.coalesce(1).write.format("csv").mode("overwrite").save(args(2))
    }
    else if(args(0)=="verify") {
      if(args.length != 3) {
        println("Usage: project_3 verify graph_path MIS_path")
        sys.exit(1)
      }

      val edges = sc.textFile(args(1)).map(line => {val x = line.split(","); Edge(x(0).toLong, x(1).toLong , 1)} )
      val vertices = sc.textFile(args(2)).map(line => {val x = line.split(","); (x(0).toLong, x(1).toInt) })
      val g = Graph[Int, Int](vertices, edges, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)

      val ans = verifyMIS(g)
      if(ans)
        println("Yes")
      else
        println("No")
    }
    else
    {
        println("Usage: project_3 option = {compute, verify}")
        sys.exit(1)
    }
  }
}
