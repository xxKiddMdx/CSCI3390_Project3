package project_3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}

object main{
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)

  case class VertexProperties(id: Long, degree: Int, value: Double, active: String, in_MIS: String)

  def anyActive(g:Graph[VertexProperties, Int]): Boolean = {
    g.vertices.filter{ case (id, vp) => vp.active == "active"}.count() > 0
  }

  def inRDD(rdd: RDD[Long], value: Long): Boolean = {
    rdd.filter(x => x == value).count() > 0
  }

  def LubyMIS(g_in: Graph[Int, Int]): Graph[Int, Int] = {
    var mis_vertices = Set[Long]()
    val degrees = g_in.degrees
    var degree_graph = g_in.outerJoinVertices(degrees)({ case (_, _, prop) => prop.getOrElse(0)})
    var g_mod = degree_graph.mapVertices((id, degree) => VertexProperties(id, degree, -0.1, "active", "No"))
    g_mod.cache()

    var iterations = 0

    while(anyActive(g_mod)){
      g_mod = g_mod.mapVertices((_, prop: VertexProperties) => {
        if (prop.active != "active") {
          prop
        } else {
          val rand = new scala.util.Random
          VertexProperties(prop.id, prop.degree, rand.nextDouble(), prop.active, prop.in_MIS)
        }
      })

      val highest_neighbor: VertexRDD[VertexProperties] = g_mod.aggregateMessages[VertexProperties](
        triplet => {
          triplet.sendToDst(triplet.srcAttr)
          triplet.sendToSrc(triplet.dstAttr)
        },
        (prop1, prop2) => {
          if (prop1.value > prop2.value) {
            prop1
          } else {
            prop2
          }
        }
      )

      val joinedVertices = g_mod.vertices.join(highest_neighbor).filter({
        case (_, (prop, competing)) => prop.active == "active"
      })

      val message_comparison = joinedVertices.filter({
        case (id, (prop: VertexProperties, competing: VertexProperties)) => {
          prop.value > competing.value
        }
      })

      val vertexIds_mis = message_comparison.map({ case ((id, (prop, competing))) => prop.id}).distinct().collect().toSet
      val in_mis_graph = g_in.mapVertices((id, _) => vertexIds_mis.contains(id))
      val neighbor_in_mis: VertexRDD[Boolean] = in_mis_graph.aggregateMessages[Boolean](
        triplet => {
          triplet.sendToDst(triplet.srcAttr)
          triplet.sendToSrc(triplet.dstAttr)
        },
        (a, b) => a || b
      )
      val mis_neighbors = neighbor_in_mis.filter({ case (id, in_mis) => in_mis}).map({ case (id, _) => id}).distinct().collect().toSet

      g_mod = g_mod.mapVertices((id, prop) => {
        if (vertexIds_mis.contains(id)) {
          VertexProperties(prop.id, prop.degree, -0.1, "inactive", "Yes")
        } else if (mis_neighbors.contains(id)) {
          VertexProperties(prop.id, prop.degree, -0.1, "inactive", "No")
        } else {
          prop
        }
      })
      println("\tNumber of vertices remaining: " + g_mod.vertices.filter({ case (id, prop) => prop.active == "active"}).count())
      iterations += 1
    }
    println("Number of iterations: " + iterations)
    val vertexRDD = g_mod.vertices

    val verticeInMIS: RDD[VertexId] = vertexRDD
      .filter{ case (_, prop) => prop.in_MIS == "Yes"}
      .map { case (id, _) => id}

    val vertexIDs: Array[VertexId] = verticeInMIS.collect()
    val out_graph = g_in.mapVertices((id, _) => if (vertexIDs.contains(id)) 1 else -1)
    return out_graph
  }

def hasNeighborWithAttributeOne(triplet: EdgeContext[Int, Int, Boolean], from: String): Boolean = {
      if (triplet.srcAttr == 1 && from == "to_dst") {
      return true
    } else if (triplet.srcAttr == -1 && from == "to_dst") {
      return false
    } else if (triplet.dstAttr == 1 && from == "to_src") {
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