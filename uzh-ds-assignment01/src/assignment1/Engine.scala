package assignment1

object Engine {
  def main(args: Array[String]) {
    val webServerPort = 8080
    val textServerPort = args(0).toInt
    val webServer = new WebServer(webServerPort, textServerPort)
    val textServer = new TextProcessingServer(textServerPort)
  }
}