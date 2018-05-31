package example

import common.SessionUtil

object SparkReadFromJSON {

  private final val localPath = s"file:///Users/lipeng/workspace_mryx/umich/src/main/scala/example/data/output/dim_base_station.json";

  def main(args: Array[String]): Unit = {
    val spSession = SessionUtil.sparkSession()
    spSession.read.format("json").load(localPath).show()
  }
}
