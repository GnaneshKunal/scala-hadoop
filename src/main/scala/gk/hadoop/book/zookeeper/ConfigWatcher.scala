package gk.hadoop.book.zookeeper

import org.apache.zookeeper.Watcher.Event.EventType
import org.apache.zookeeper._

class ConfigWatcher(hosts: String) extends Watcher{

  val store = new ActiveKeyValueStore()
  store.connect(hosts)

  def displayConfig() {
    val value = store.read(ConfigUpdater.PATH, this)
    println {
      "Read " + ConfigUpdater.PATH + " as " + value
    }
  }

  def process(event: WatchedEvent): Unit = {
    if (event.getType() == EventType.NodeDataChanged) {
      try {
        displayConfig()
      } catch {
        case e: InterruptedException =>
          println("Interrupted Exiting")
          Thread.currentThread().interrupt()
        case e: KeeperException =>
          println {
            "KeeperException: " + e + ". Exiting."
          }
      }
    }
  }
}

object ConfigWatcher {

  def main(args: Array[String]): Unit = {
    val configWatcher = new ConfigWatcher(args(0))
    configWatcher.displayConfig()

    Thread.sleep(100000000)
  }

}
