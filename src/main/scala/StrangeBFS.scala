import org.apache.spark.SparkContext

object StrangeBFS {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]", "StrangeBFS")
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

    var graph = whiteGraph.map(e => {
      if (e.id == exploredId) {
        e.copy(w = -1, color = GRAY)
      } else {
        e
      }
    })

    var result: Map[String, Int] = Map.empty//(vertex, weight)

    def evalGrayList() = graph.filter(_.color == GRAY).collect().toList

    var grayList = evalGrayList()

    def graph2Str(): String = {
      s"${System.lineSeparator()}${graph.collect().toList.mkString(System.lineSeparator())}"
    }

    while (grayList.nonEmpty) {
      graph = graph.map(e => {
        if (grayList.map(_.id).contains(e.id)) {
          e.copy(w = grayList.filter(_.id == e.id).head.w + 1, color = BLACK)
        } else {
          e
        }
      })

      val blacked = graph.filter(e => grayList.map(_.id).contains(e.id)).collect().toList//bfs nodes
      val id2W = blacked.map(e => (e.idList, e.w)).flatMap(e => e._1.map(s => s -> e._2)).toMap

      //bcz all paths have the same weight
      result = result ++ blacked.map(e => e.id -> e.w).toMap

      graph = graph.map(e => {
        if (id2W.keys.toSet.contains(e.id) && e.color == WHITE) {
          e.copy(w = id2W(e.id), color = GRAY)
        } else {
          e
        }
      })

      //look here!!!
      println(s"before filtering: ${graph2Str()}")

      grayList = evalGrayList()

      println(s"after filtering: ${graph2Str()}")
    }

    println(s"result: ${System.lineSeparator()}")
    result.foreach(println)

    sc.stop()
  }
}
