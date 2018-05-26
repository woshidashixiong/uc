package example

import common.util

object SparkWriteToCSV extends Serializable {

  private final val sql = s"select * from bi_dw.dim_base_station limit 10";
  private final val localPath = s"file:///Users/lipeng/workspace_mryx/umich/src/main/scala/example/data/output/dim_base_station.csv";

  private final val tableName = "dw_test:test"
  private final val tableColumnFamily = "info"

  def main(args: Array[String]): Unit = {
    val spSession = util.spark_session()

    val baseStationDf = spSession.sql(sql)
    baseStationDf.show()
    println("-----------------------------------------------------------------------")

    baseStationDf
      .select(baseStationDf.col("id"),
        baseStationDf.col("name"),
        baseStationDf.col("city"),
        baseStationDf.col("district"),
        baseStationDf.col("address"))
      .write
      .option("hader", true)
      .option("delimiter", "\t")
      .csv(localPath)

    spSession.stop()
  }
}
