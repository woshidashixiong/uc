package example

import common.SessionUtil

object SparkReadFromCSV {

  private final val localPath = s"file:///Users/lipeng/workspace_mryx/umich/src/main/scala/example/data/output/dim_base_station.csv";

  def main(args: Array[String]): Unit = {
    val spSession = SessionUtil.sparkSession()
    spSession.sqlContext.read.format("csv").option("header", "false").load(localPath).show()
  }
}
