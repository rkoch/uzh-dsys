package assignment1
import java.net.InetSocketAddress
import com.sun.net.httpserver.{ HttpExchange, HttpHandler, HttpServer }
import java.util.concurrent.Executors

class WebServer(port: Int) {
  val address = new InetSocketAddress(port)
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
  //TODO(Student): You have to implement this. You can remove the if-else block. See instructions for more information
  def getResult(query: String): String = {
    if (query == "pigs")
      "1465 2319"
    else
      "it works!"
  }
}