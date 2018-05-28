package example

import common.util

object SparkReadFromJSON {

  private final val localPath = s"file:///Users/lipeng/workspace_mryx/umich/src/main/scala/example/data/output/dim_base_station.json";

  def main(args: Array[String]): Unit = {
    val spSession = util.spark_session()
    spSession.read.format("json").load(localPath).show()
  }
}
