package gk.hadoop.book.zookeeper

import java.util.concurrent.TimeUnit

import scala.util.Random

class ConfigUpdater(hosts: String) {

  import ConfigUpdater._

  private var store: ActiveKeyValueStore = new ActiveKeyValueStore()
  private val random = new Random()
  store.connect(hosts)


  def run(): Unit = {
    while (true) {
      val value = random.nextInt(1000) + ""
      store.write(PATH, value)
      println {
        "Set " + PATH + " to " + value  
      }
      TimeUnit.SECONDS.sleep(random.nextInt(10))
    }
  }
}


object ConfigUpdater {

  val PATH = "/config"

  def main(args: Array[String]): Unit = {
    val configUpdater = new ConfigUpdater(args(0))
    configUpdater.run()
  }

}
