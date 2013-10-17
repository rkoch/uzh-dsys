package assignment1

import java.net.Socket
import java.net.InetAddress
import scala.io.BufferedSource
import java.io.PrintStream

object Engine {
  def main(args: Array[String]) {
    val webServerPort = 8080
    val textServerPort = args(0).toInt
    val webServer = new WebServer(webServerPort)
    val textServer = new TextProcessingServer(textServerPort)
    test
  }

  def test() {
    val result = "1465 2319"
    val queryResult = RetrievalSystem.getResult("pigs")
    if (queryResult == result) println("passes test")
    else println("fails test")
  }
}