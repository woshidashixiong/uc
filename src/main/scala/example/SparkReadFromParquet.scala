package example

import common.util

object SparkReadFromParquet {

  private final val localPath = s"file:///Users/lipeng/workspace_mryx/umich/src/main/scala/example/data/output/dim_base_station.parquet";

  def main(args: Array[String]): Unit = {
    val spSession = util.spark_session()
    spSession.read.format("parquet").load(localPath).show()
  }
}
