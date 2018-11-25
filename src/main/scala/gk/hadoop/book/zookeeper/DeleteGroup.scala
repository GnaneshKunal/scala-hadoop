package gk.hadoop.book.zookeeper

import org.apache.zookeeper._

class DeleteGroup extends ConnectionWatcher {

  def delete(groupName: String): Unit = {
    val path = "/" + groupName

    try {
      val children = zk.getChildren(path, false)
      val childitr = children.iterator
      while (childitr.hasNext())
        zk.delete(path + "/" + childitr.next(), -1)
      zk.delete(path, -1)
    } catch {
      case e: KeeperException.NoNodeException =>
        println("Group: " + groupName + " doesn't exist")
        System.exit(1)
    }
  }

}

object DeleteGroup {
  def main(args: Array[String]): Unit = {
    val deleteGroup = new DeleteGroup()
    deleteGroup.connect(args(0))
    deleteGroup.delete(args(1))
    deleteGroup.close()
  }
}
