package example

import com.fasterxml.jackson.databind.ObjectMapper
import common.{hbase_util, util}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

object SparkWriteToHDFS extends Serializable {

  private final val sql = s"select * from bi_dw.dim_base_station limit 10";
  private final val localPath = s"file:///Users/lipeng/workspace_mryx/umich/src/main/scala/example/data/output/dim_base_station.json";

  def main(args: Array[String]): Unit = {
    val spSession = util.spark_session()


    val baseStationDf = spSession.sql(sql)
    baseStationDf.show()

    println("-----------------------------------------------------------------------")

    val objectMapper = new ObjectMapper()

    baseStationDf.rdd.map(row => {
      val objectNode = objectMapper.createObjectNode()
      objectNode.put("id", row.getAs[Int]("id"))
      objectNode.put("name", row.getAs[String]("name"))
      objectNode.put("city", row.getAs[String]("city"))
      objectNode.put("district", row.getAs[String]("district"))
      objectNode.put("address", row.getAs[String]("address"))
      objectNode.toString
    }).saveAsTextFile(localPath)

    spSession.stop()
  }
}
