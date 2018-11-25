package gk.hadoop.book.zookeeper

import java.util.concurrent.CountDownLatch

import org.apache.zookeeper._
import org.apache.zookeeper.Watcher.Event.KeeperState

class ConnectionWatcher extends Watcher {
  private [this] val SESSION_TIMEOUT = 5000

  protected var zk: ZooKeeper = _

  private [this] val connectedSignal = new CountDownLatch(1)

  def connect(hosts: String): Unit = {
    zk = new ZooKeeper(hosts, SESSION_TIMEOUT, this)
    connectedSignal.await()
  }

  def process(event: WatchedEvent): Unit = {
    if (event.getState() == KeeperState.SyncConnected)
      connectedSignal.countDown()
  }

  def close(): Unit = {
    zk.close()
  }

}
