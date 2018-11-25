package gk.hadoop.book.zookeeper

import org.apache.zookeeper._
import org.apache.zookeeper.ZooDefs.Ids

class JoinGroup extends ConnectionWatcher {

  def join(groupName: String, memberName: String): Unit = {
    val path = "/" + groupName + "/" + memberName
    val createdPath = zk.create(path, null, Ids.OPEN_ACL_UNSAFE,
      CreateMode.PERSISTENT)
    println {
      "Created " + createdPath
    }
  }
}

object JoinGroup {
  def main(args: Array[String]): Unit = {
    val joinGroup = new JoinGroup()
    joinGroup.connect(args(0))
    joinGroup.join(args(1), args(2))

    // Thread.sleep(1000000000)
  }
}
