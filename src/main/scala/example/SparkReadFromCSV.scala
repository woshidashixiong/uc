package example

import common.util

object SparkReadFromCSV {

  private final val localPath = s"file:///Users/lipeng/workspace_mryx/umich/src/main/scala/example/data/output/dim_base_station.csv";

  def main(args: Array[String]): Unit = {
    val spSession = util.spark_session()
    spSession.sqlContext.read.format("csv").option("header", "false").load(localPath).show()
  }
}
