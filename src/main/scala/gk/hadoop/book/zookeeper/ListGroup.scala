package gk.hadoop.book.zookeeper

import org.apache.zookeeper._

class ListGroup extends ConnectionWatcher {
  def list(groupName: String): Unit = {
    val path = "/" + groupName

    try {
      val children = zk.getChildren(path, false)
      if (children.isEmpty()) {
        println {
          "No members in group " + groupName + "\n"
        }
        System.exit(1)
      }

      val childitr = children.iterator

      while (childitr.hasNext())
        println(childitr.next())

    } catch {
      case e: KeeperException.NoNodeException =>
        println("Group " + groupName + " does not exist\n")
    }
  }
}

object ListGroup {
  def main(args: Array[String]): Unit = {
    val listGroup = new ListGroup()
    listGroup.connect(args(0))
    listGroup.list(args(1))
    listGroup.close()
  }
}
