package turnover

import com.bjtuling.utils.DateUtil
import common.{HbasePutUtil, HbaseUtil, SessionUtil}
import org.apache.commons.lang3.math.NumberUtils
import org.apache.hadoop.hbase.TableName
import org.apache.log4j.Logger
import enumeration.HbaseEnum.{TableAIUmichEnum => TableEnum}
import enumeration.HbaseEnum.{ColumnAIUmichUmichProductLableEnum => ColumnEnum}
import enumeration.HbaseEnum.{NamespaceEnum => DatebaseEnum}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import turnover.DailyMeanSales.logger

object ReplenishmentQuantity extends Serializable {

  private final val logger = Logger.getLogger(ReplenishmentQuantity.getClass)

  private def init(args: Array[String]): Tuple3[String, String, String] = {
    var dateInterval = "7"
    var currentDate = ""
    if (null != args && !args.isEmpty) {
      if (NumberUtils.isDigits(args(0).trim)) {
        logger.error(" The interval of data is not a number !")
        System.exit(-1)
      }
      dateInterval = args(0).trim

      currentDate = if (args.length >= 2) {
        args(1)
      } else {
        DateUtil.getYesterday()
      }
    } else {
      currentDate = DateUtil.getYesterday()
      logger.warn(" The argument about dateInterval and currentDate are both null.")
    }

    val sql =
      s"""|	SELECT
          |		r.goods_code AS r_goods_code,
          |		SUM(r.receive_count) AS total_replenishment_num
          |	FROM
          |		bi_dw.fact_erp_replenishment_data AS r
          |	WHERE
          |	    r.status = 1
          |		AND to_date(r.receive_time) BETWEEN date_sub('${currentDate}', ${dateInterval}) AND date_sub('${currentDate}', 1)
          |	GROUP BY
          |		r.goods_code""".stripMargin;

    (sql, currentDate, dateInterval)
  }

  def main(args: Array[String]): Unit = {
    val (sql, currentDate, dateInterval) = init(args)

    logger.info(" currentDate = " + currentDate)
    logger.info(" dateInterval = " + dateInterval)


    val spSession = SessionUtil.sparkSession()
    val df = spSession.sql(sql)
    val keyPrefix = currentDate.replaceAll("-", "").reverse
    val keyPrefixBC = spSession.sparkContext.broadcast(keyPrefix)
    // TODO would be deleted
    println(" currentDate = " + currentDate)
    println(" dateInterval = " + dateInterval)
    println(" sql = " + sql)
    println(" keyPrefix = " + keyPrefix)

    df.foreachPartition(partItr => {
      val conn = HbaseUtil.createHbaseConnection()

      // table
      import DatebaseEnum._
      import TableEnum._
      val tableName = TableName.valueOf(AI_UMICH.toString + HbasePutUtil.KEY_SEPARATOR + UMICH_PRODUCT_LABEL.toString)
      val hbaseTable = conn.getBufferedMutator(tableName)

      import ColumnEnum._
      // column family
      val turnoverColFlyName = CF_OPERATION.toString
      val turnoverColFlyByte = getColumnsBytes(turnoverColFlyName).get

      // columns
      val goodsCodeName = GOODS_CODE.toString
      val goodsCodeByte = getColumnsBytes(goodsCodeName).get

      val totalReplenishmentNumName = TOTAL_REPLENISHMENT_NUM.toString
      val totalReplenishmentNumByte = getColumnsBytes(totalReplenishmentNumName).get

      partItr.foreach(item => {
        val goodsCode = item.getAs[String](goodsCodeName)
        val rowKeyByte = Bytes.toBytes(keyPrefixBC.value + HbasePutUtil.KEY_SEPARATOR + goodsCode)

        val put = new Put(rowKeyByte)
        put.addColumn(turnoverColFlyByte, goodsCodeByte, Bytes.toBytes(goodsCode))
        put.addColumn(turnoverColFlyByte, totalReplenishmentNumByte, Bytes.toBytes(item.getAs[Double](totalReplenishmentNumName)))
        hbaseTable.mutate(put)

      })

      hbaseTable.flush()
      hbaseTable.close()
    })

    spSession.stop()
  }
}
