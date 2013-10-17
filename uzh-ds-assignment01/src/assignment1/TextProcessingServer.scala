package assignment1

import scala.collection.{ mutable => mut }
import scala.io.Codec
import scala.io.Source
import com.sun.net.httpserver.HttpServer
import java.net.InetSocketAddress
import java.util.concurrent.Executors

//TODO(Student): You have to implement this
class TextProcessingServer(port: Int) {
  val address = new InetSocketAddress(port)
  val server = HttpServer.create(address, 0)
  server.createContext("/", new WorkerStateRequestHandler())
  server.setExecutor(Executors.newCachedThreadPool())
  server.start

  def shutdown() {
    server.stop(0)
  }
  println("hello?")

  println("lewis carroll: " + InvertedIndex.get("lewis carroll"))
  println("alice in wonderland: " + InvertedIndex.get("alice in wonderland"))
  println("web site search: " + InvertedIndex.get("web site search"))
  println("goldfish: " + InvertedIndex.get("goldfish"))
  println("queen king: " + InvertedIndex.get("queen king"))

  /*
   * "lewis carroll" "alice in wonder- land"
"web site search" "goldfish"
"queen king"
   */
}

object InvertedIndex {
  println("constructing inverted index")
  val rsc = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("pg11.txt"))(Codec.UTF8)
  val lineArr = rsc.getLines.mkString("\n").filterNot("()'$:\";,[].!?".toSet).toLowerCase.split("\n")

  val index: mut.Map[String, mut.Set[Int]] = mut.Map();
  for (i <- 0 until lineArr.length) {
    val splitLine = lineArr(i).split(" ")
    for (word <- splitLine) {
      val opt = index.get(word)
      if (!opt.isDefined) {
        index.put(word, mut.Set())
      }
      index(word).add(i + 1)
    }
  }

  rsc.close

  def get(query: String): String = {
    val split = query.split(" ")

    val list = mut.Set[mut.Set[Int]]()

    for (word <- split) {
      val opt = index.get(word)
      if (opt.isDefined) {
        list += opt.get
      }
    }

    var res = mut.Set[Int]()
    for (entry <- list) {
      if (res.isEmpty) {
        res = entry
      } else {
        res = res.intersect(entry)
      }
    }

    res.mkString(" ")
  }

}
