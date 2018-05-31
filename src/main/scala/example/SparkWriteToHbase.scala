package example

import common.SessionUtil
import common.HbaseUtil
import enumeration.HbaseEnum
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.Row

object SparkWriteToHbase extends Serializable {

  private final val sql = s"select * from bi_dw.dim_base_station limit 10";

  private final val tableName = "dw_test:test"
  private final val tableColumnFamily = "info"

  def main(args: Array[String]): Unit = {
    val spSession = SessionUtil.sparkSession()


    val baseStationDf = spSession.sql(sql)
    baseStationDf.show()

    println("-----------------------------------------------------------------------")
    baseStationDf.foreachPartition(partItr => {
      // connection
      val conn = HbaseUtil.createHbaseConnection()

      // table
      val hbaseTableName = TableName.valueOf(tableName)
      val m = conn.getBufferedMutator(hbaseTableName)

      // column
      val columnFamilyByte = Bytes.toBytes(tableColumnFamily)
      val nameByte = Bytes.toBytes("name")
      val cityByte = Bytes.toBytes("city")
      val districtByte = Bytes.toBytes("district")
      val addressByte = Bytes.toBytes("address")

      partItr.foreach(r => {
        val rowKeyByte = Bytes.toBytes(r.getAs[Int]("id"))
        val put = new Put(rowKeyByte)
        put.addColumn(columnFamilyByte, nameByte, Bytes.toBytes(r.getAs[String]("name")))
        put.addColumn(columnFamilyByte, cityByte, Bytes.toBytes(r.getAs[String]("city")))
        put.addColumn(columnFamilyByte, districtByte, Bytes.toBytes(r.getAs[String]("district")))
        put.addColumn(columnFamilyByte, addressByte, Bytes.toBytes(r.getAs[String]("address")))
        m.mutate(put)
      })

      m.flush()
      m.close()
    })

    spSession.stop()
  }
}
