import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.annotation.tailrec

object BFS {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]", "BFS")

    val raw = sc.textFile(args(0))

    val WHITE = "WHITE"
    val GRAY = "GRAY"
    val BLACK = "BLACK"

    val INF = 9999

    case class BFSNode(id: String, idList: List[String], w: Int, color: String)

    val whiteGraph = raw.map(s => {
      val ss = s.split(" ").toList
      BFSNode(id = ss.head, idList = ss.tail, w = INF, color = WHITE)
    })

    val exploredId = args(1)

    val graph: RDD[BFSNode] = whiteGraph.map(e => {
      if (e.id == exploredId) {
        e.copy(w = -1, color = GRAY)
      } else {
        e
      }
    })

    def evalGrayList(graph: RDD[BFSNode]) = graph.filter(_.color == GRAY).collect().toList

    val grayList = evalGrayList(graph)

    val result0: Map[String, Int] = Map.empty//(vertex, weight)


    @tailrec
    def bfs(grayList: List[BFSNode], graph: RDD[BFSNode], result: Map[String, Int]): Map[String, Int] = {
      if (grayList.nonEmpty) {
        val graph0 = graph.map(e => {
          if (grayList.map(_.id).contains(e.id)) {
            e.copy(w = grayList.filter(_.id == e.id).head.w + 1, color = BLACK)
          } else {
            e
          }
        })

        val blacked = graph0.filter(e => grayList.map(_.id).contains(e.id)).collect().toList//bfs nodes
        val id2W = blacked.map(e => (e.idList, e.w)).flatMap(e => e._1.map(s => s -> e._2)).toMap//id to weight (map)

        //bcz all paths have the same weight
        val r = result ++ blacked.map(e => e.id -> e.w).toMap

        val graph1 = graph0.map(e => {
          if (id2W.keys.toSet.contains(e.id) && e.color == WHITE) {
            e.copy(w = id2W(e.id), color = GRAY)
          } else {
            e
          }
        })

        val grayListUpd = evalGrayList(graph1)

        bfs(grayListUpd, graph1, r)
      } else {
        val stillWhite = graph.filter(_.color == WHITE).map(e => (e.id, 0)).collect().toMap
        result ++ stillWhite
      }
    }

    val result = bfs(grayList, graph, result0)

    val r = result.toList.map(e => (e._2, e._1)).groupBy(_._1)
      .map(e => (e._1, e._2.map(_._2))).toList
      .sortWith((v1, v2) => {
        v1._1 < v2._1
      })

    println(s"result:")
    r.foreach(println)

    sc.stop()
  }
}
