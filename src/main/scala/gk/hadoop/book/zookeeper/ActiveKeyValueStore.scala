package gk.hadoop.book.zookeeper

import java.nio.charset.Charset

import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.Watcher


class ActiveKeyValueStore extends ConnectionWatcher {

  private [this] val CHARSET = Charset.forName("UTF-8")

  def write(path: String, value: String): Unit = {
    val stat = zk.exists(path, false)
    if (stat == null) {
      zk.create(path, value.getBytes(CHARSET), Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT)
    } else {
      zk.setData(path, value.getBytes(CHARSET), - 1)
    }
  }

  def read(path: String, watcher: Watcher) = {
    val data = zk.getData(path, watcher, null)

    new String(data, CHARSET)
  }

}
