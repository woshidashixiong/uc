package turnover

import common.{HbasePutUtil, HbaseUtil, SessionUtil}
import enumeration.HbaseEnum.{NamespaceEnum => DatabaseEnum}
import enumeration.HbaseEnum.{TableAIUmichEnum => TableEnum}
import enumeration.HbaseEnum.{ColumnAIUmichUmichProductLableEnum => ColumnEnum}
import org.apache.log4j.Logger
import com.bjtuling.utils.DateUtil
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

object ReplenishmentQuantity3Weeks extends Serializable {

  private val logger: Logger = Logger.getLogger(ReplenishmentQuantity3Weeks.getClass)

  private def init(args: Array[String]): Tuple2[String, String] = {

    var currentDate = if (null != args && !args.isEmpty) {
      args(0)
    } else {
      logger.warn(" The argument currentDate is null.")
      DateUtil.getYesterday()
    }

    val sql =
      s"""
         |SELECT
         |	T1.r_goods_code AS goods_code,
         |	(T1.first_week_total_rep_num + T1.second_week_total_rep_num + T1.third_week_total_rep_num) /
         |	(3-(CASE WHEN T1.first_week_total_rep_num = 0 THEN 1 ELSE 0 END +
         |	CASE WHEN T1.second_week_total_rep_num = 0 THEN 1 ELSE 0 END +
         |	CASE WHEN T1.third_week_total_rep_num = 0 THEN 1 ELSE 0 END)) AS avg_week_rep_num
         |FROM (
         |	SELECT
         |		r.goods_code AS r_goods_code,  -- 商品编码
         |		SUM(CASE WHEN to_date(r.receive_time) BETWEEN date_sub('${currentDate}', 7) AND date_sub('${currentDate}', 1) THEN r.receive_count ELSE 0 END) AS first_week_total_rep_num,
         |		SUM(CASE WHEN to_date(r.receive_time) BETWEEN date_sub('${currentDate}', 14) AND date_sub('${currentDate}', 8) THEN r.receive_count ELSE 0 END) AS second_week_total_rep_num,
         |		SUM(CASE WHEN to_date(r.receive_time) BETWEEN date_sub('${currentDate}', 21) AND date_sub('${currentDate}', 15) THEN r.receive_count ELSE 0 END) AS third_week_total_rep_num
         |	FROM
         |		bi_dw.fact_erp_replenishment_data AS r
         |	WHERE
         |	    r.status = 1
         |		AND to_date(r.receive_time) BETWEEN date_sub('${currentDate}', 21) AND date_sub('${currentDate}', 1)
         |	GROUP BY
         |		r.goods_code
         |	) AS T1
      """.stripMargin
    null
  }

  def main(args: Array[String]): Unit = {

    val (sql, currentDate) = init(args)
    logger.info(" currentDate = " + currentDate)

    val spSession = SessionUtil.sparkSession()
    val df = spSession.sql(sql)

    val keyPrefix = currentDate.replaceAll("-", "").reverse
    val keyPrefixBC = spSession.sparkContext.broadcast(keyPrefix)

    // TODO would be deleted
    println(" currentDate = " + currentDate)
    println(" sql = " + sql)
    println(" keyPrefix = " + keyPrefix)

    df.foreachPartition(partItr => {
      val conn = HbaseUtil.createHbaseConnection()

      import DatabaseEnum._
      import TableEnum._
      val tableName = TableName.valueOf(AI_UMICH.toString + HbasePutUtil.KEY_SEPARATOR + UMICH_PRODUCT_LABEL.toString)
      val hbaseTable = conn.getBufferedMutator(tableName)

      import ColumnEnum._
      val turnoverColFlyName = CF_TURNOVER.toString
      val turnoverColFlyByte = getColumnsBytes(turnoverColFlyName).get

      val goodsCodeName = GOODS_CODE.toString
      val goodsCodeByte = getColumnsBytes(goodsCodeName).get

      val avgWeekRepNumName = AVG_WEEK_REP_NUM.toString
      val avgWeekRepNumByte = getColumnsBytes(avgWeekRepNumName).get

      partItr.foreach(item => {
        val goodsCode = item.getAs[String](goodsCodeName)
        val rowKey = Bytes.toBytes(keyPrefixBC.value + HbasePutUtil.KEY_SEPARATOR + goodsCode)

        val put = new Put(rowKey)
        put.addColumn(turnoverColFlyByte, goodsCodeByte, Bytes.toBytes(goodsCode))
        put.addColumn(turnoverColFlyByte, avgWeekRepNumByte, Bytes.toBytes(item.getAs[Double](avgWeekRepNumName)))
        hbaseTable.mutate(put)
      })

      hbaseTable.flush()
      hbaseTable.close()
    })

    spSession.stop()
  }
}
