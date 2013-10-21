package assignment1
import java.net.InetSocketAddress
import com.sun.net.httpserver.{ HttpExchange, HttpHandler, HttpServer }
import java.util.concurrent.Executors
import java.net.URLDecoder
import scala.io.Codec
import java.net.Socket
import java.io.PrintWriter
import java.io.BufferedReader
import java.io.InputStreamReader

class WebServer(port: Int, pTextPort: Int) {
  RetrievalSystem.init(pTextPort)
  val address = new InetSocketAddress(port)
  val textPort = pTextPort;
  val server = HttpServer.create(address, 0)
  server.createContext("/", new WorkerStateRequestHandler())
  server.setExecutor(Executors.newCachedThreadPool())
  server.start
  println("Web Server started on localhost:" + address.getPort)

  def shutdown {
    server.stop(0)
  }
}

class WorkerStateRequestHandler() extends HttpHandler {
  def handle(exchange: HttpExchange) {
    val requestMethod = exchange.getRequestMethod
    if (requestMethod.equalsIgnoreCase("GET")) {
      val queryResult = RetrievalSystem.getResult(exchange.getRequestURI().getRawQuery().replaceFirst("q=", ""))
      val responseHeaders = exchange.getResponseHeaders
      responseHeaders.set("Content-Type", "text/html")
      responseHeaders.set("Access-Control-Allow-Origin", "*");
      exchange.sendResponseHeaders(200, queryResult.length())

      val responseBody = exchange.getResponseBody
      responseBody.write(queryResult.getBytes())
      responseBody.close
      exchange.close()
    }
  }
}

object RetrievalSystem {
  var port = 1337
  var socket: Socket = null

  def init(pPort: Int) {
    port = pPort
  }

  def getResult(query: String): String = {
    // URL Decode the input query
    val decodedQuery = URLDecoder.decode(query, Codec.UTF8.toString)

    if (socket == null) {
      socket = new Socket("localhost", port)
    }

    val out = new PrintWriter(socket.getOutputStream())
    val in = new BufferedReader(new InputStreamReader(socket.getInputStream()))
    out.println(decodedQuery);
    out.flush()

    // Read return
    val ret = in.readLine

    // return value
    ret
  }

}
