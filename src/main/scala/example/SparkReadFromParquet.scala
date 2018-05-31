package example

import common.SessionUtil

object SparkReadFromParquet {

  private final val localPath = s"file:///Users/lipeng/workspace_mryx/umich/src/main/scala/example/data/output/dim_base_station.parquet";

  def main(args: Array[String]): Unit = {
    val spSession = SessionUtil.sparkSession()
    spSession.read.format("parquet").load(localPath).show()
  }
}
