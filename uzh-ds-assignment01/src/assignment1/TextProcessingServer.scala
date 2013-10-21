package assignment1

import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.PrintStream
import java.net.ServerSocket
import java.net.Socket

import scala.actors.Actor
import scala.collection.{ mutable => mut }
import scala.io.Codec
import scala.io.Source

class TextProcessingServer(port: Int) {
  private case class Client(socket: Socket, is: BufferedReader, os: PrintStream)
  private val connectionPool = new mut.HashSet[Client] with mut.SynchronizedSet[Client]
  private var doShutdown = false

  private val listener = new ServerSocket(port)
  Actor.actor {
    while (!doShutdown) { // infinite loop -> waiting for connection attempts
      val socket = listener.accept()
      connectionPool += Client(socket, new BufferedReader(new InputStreamReader(socket.getInputStream())), new PrintStream(socket.getOutputStream()));
    }
  }

  Actor.actor {
    while (!doShutdown) {
      connectionPool.retain((client) => {
        if (client.socket.isConnected()) {
          if (client.is.ready) {
            val query = client.is.readLine
            client.os.println(InvertedIndex.get(query));
          }
          true
        } else {
          false
        }
      })

      // short timeout to lower cpu usage
      Thread.sleep(50)
    }
  }

  def shutdown() {
    doShutdown = true;
    listener.close();
    for (client <- connectionPool) {
      client.socket.close();
    }
  }

}

object InvertedIndex {
  val rsc = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("pg11.txt"))(Codec.UTF8)
  val lineArr = rsc.getLines.mkString("\n").filterNot("()'$:\";,[].!?".toSet).toLowerCase.split("\n")

  val index: mut.Map[String, mut.Set[Int]] = mut.Map()
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
