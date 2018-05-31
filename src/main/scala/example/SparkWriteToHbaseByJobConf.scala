package example

import common.{HbaseUtil, SessionUtil}
import org.apache.hadoop.classification.InterfaceAudience
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark


@InterfaceAudience.Private
case class HBaseRecord(id: Int,
                       name: String,
                       city: String,
                       district: String,
                       address: String)

object SparkWriteToHbaseByJobConf extends Serializable {


  private final val sql = s"select * from bi_dw.dim_base_station limit 20";

  private final val tableName = "dw_test:test"
  private final val tableColumnFamily = "info"

  def main(args: Array[String]): Unit = {
    val spSession = SessionUtil.sparkSession()
    val hbaseConf = HbaseUtil.createHbaseConfiguration()

    val jobConf = new JobConf(hbaseConf, this.getClass)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    val baseStationDf = spSession.sql(sql)
    baseStationDf.show()

    val columnFamilyByte = Bytes.toBytes(tableColumnFamily)
    val nameByte = Bytes.toBytes("name")
    val cityByte = Bytes.toBytes("city")
    val districtByte = Bytes.toBytes("district")
    val addressByte = Bytes.toBytes("address")

    val hbaseDf = baseStationDf.rdd.map(row => {
      val rowKeyByte = Bytes.toBytes(row.getAs[Int]("id"));
      val put = new Put(rowKeyByte)
      put.addColumn(columnFamilyByte, nameByte, Bytes.toBytes(row.getAs[String]("name")))
      put.addColumn(columnFamilyByte, cityByte, Bytes.toBytes(row.getAs[String]("city")))
      put.addColumn(columnFamilyByte, districtByte, Bytes.toBytes(row.getAs[String]("district")))
      put.addColumn(columnFamilyByte, addressByte, Bytes.toBytes(row.getAs[String]("address")))

      (new ImmutableBytesWritable(rowKeyByte), put)
    })

    hbaseDf.saveAsHadoopDataset(jobConf)
  }
}
