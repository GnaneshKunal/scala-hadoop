package gk.hadoop.book.zookeeper

//import org.apache.curator.utils.
import java.util.concurrent.CountDownLatch

import org.apache.zookeeper.Watcher.Event.KeeperState
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper._

class CreateGroup extends Watcher {

  private[this] val SESSION_TIMEOUT = 5000

  private[this] var zk: ZooKeeper = _

  private[this] val connectedSignal = new CountDownLatch(1)

  def connect(hosts: String): Unit = {
    zk = new ZooKeeper(hosts, SESSION_TIMEOUT, this)
    connectedSignal.await()
  }

  def process(event: WatchedEvent): Unit = {
    if (event.getState() == KeeperState.SyncConnected)
      connectedSignal.countDown()
  }

  def create(groupName: String): Unit = {
    val path = "/" + groupName
    val createdPath = zk.create(path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)

    println {
      "Created " + createdPath
    }
  }

  def close(): Unit = {
    zk.close()
  }

}

object CreateGroup {
  def main(args: Array[String]): Unit = {
    val createGroup = new CreateGroup

    val connectString = "localhost"
    val createString = "zoo2"

    createGroup.connect(connectString)
    createGroup.create(createString)
    createGroup.close()
  }
}
