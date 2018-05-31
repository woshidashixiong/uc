package turnover

import com.bjtuling.utils.DateUtil
import common.{HbasePutUtil, HbaseUtil, SessionUtil}
import enumeration.HbaseEnum.{ColumnAIUmichUmichProductLableEnum => ColumnEnum}
import enumeration.HbaseEnum.{TableAIUmichEnum => TableEnum}
import enumeration.HbaseEnum.{NamespaceEnum => DatabaseEnum}
import org.apache.commons.lang3.math.NumberUtils
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.Logger

/**
  * Created by lipeng
  * 2018-05-29
  * <br/>
  * DMS-DAILY MEAN SALES
  */
object DailyMeanSales extends Serializable {

  private final val logger = Logger.getLogger(DailyMeanSales.getClass)

  private def init(args: Array[String]): Tuple3[String, String, String] = {

    var dateInterval = "7"
    var currentDate = ""
    if (null != args && !args.isEmpty) {
      if (NumberUtils.isDigits(args(0).trim)) {
        logger.error(" The interval of data is not a number !")
        System.exit(-1)
      }
      dateInterval = args(0).trim

      if (args.length >= 2) {
        currentDate = args(1)
      }
    } else {
      currentDate = DateUtil.getYesterday()
      logger.warn(" The argument about dateInterval and currentDate are both null.")
    }

    val sql =
      s"""|	SELECT
          |		T1.sp_goods_code AS goods_code,
          |		-- nvl(T2.total_sale_num, 0) AS total_sale_num,
          |		-- T1.total_sale_days AS total_sale_days,
          |		nvl(T2.total_sale_num, 0) / T1.total_sale_days AS goods_day_shelf_avg_sale_num
          |	FROM (
          |		SELECT
          |		  sp.goods_code AS sp_goods_code,
          |		  COUNT(DISTINCT CONCAT(sp.goods_shelf_code, sp.goods_code, sp.ptdate)) AS total_sale_days
          |		FROM
          |		  bi_dw.dim_shelf_product_his AS sp
          |		WHERE
          |		  sp.status = 1
          |		  AND sp.product_status = 1
          |		  AND sp.shop_code IN (
          |		    SELECT
          |		      shop.shop_code
          |		    FROM
          |		      bi_dw.dim_shop_list AS shop
          |		    WHERE
          |		      shop.city != '540200'
          |		      AND shop.dw_end_date = '2099-12-31')
          |		    AND sp.product_class_name != '虚拟商品'
          |		  AND sp.ptdate BETWEEN date_sub('${currentDate}', ${dateInterval}) AND date_sub('${currentDate}', 1)
          |		GROUP BY
          |		sp.goods_code
          |	) AS T1
          |	LEFT JOIN (
          |		SELECT
          |		  od.product_id AS od_goods_code,
          |		  SUM(od.product_num) AS total_sale_num
          |		FROM
          |		  bi_dw.fact_order_detail AS od
          |		  JOIN bi_dw.dim_product_list AS p ON (p.goods_code = od.product_id AND p.product_class_name != '虚拟商品' AND p.dw_end_date = '2099-12-31')
          |		WHERE
          |		  od.order_status = 1
          |		  AND od.ptdate BETWEEN date_sub('${currentDate}', ${dateInterval}) AND date_sub('${currentDate}', 1)
          |		GROUP BY od.product_id
          |	)  AS T2
          |	ON (T1.sp_goods_code = T2.od_goods_code)""".stripMargin;

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
      // connection
      val conn = HbaseUtil.create_hbase_connection()

      // table
      import TableEnum._
      import DatabaseEnum._
      val hbaseTableName = TableName.valueOf(AIUmich.toString + ":" + UmichProductLabel.toString)
      val hbaseTable = conn.getBufferedMutator(hbaseTableName)

      // column-family : turnover
      import ColumnEnum._
      val turnoverColFlyName = ColumnFamilyTurnover.toString
      val turnoverColFlyByte = getColumnsBytes(turnoverColFlyName).get

      // column : goods_code
      val goodsCodeColName = GoodsCode.toString
      val goodsCodeByte = getColumnsBytes(goodsCodeColName).get

      // column : goods_day_shelf_avg_sale_num
      val avgSaleNumColName = GoodsDayShelfAvgSaleNum.toString
      val avgSaleNumByte = getColumnsBytes(avgSaleNumColName).get

      // column
      partItr.foreach(r => {
        // key
        val goodsNum = r.getAs[String](goodsCodeColName)
        val rowKeyByte = Bytes.toBytes(keyPrefixBC.value + HbasePutUtil.KeySeparator + goodsNum)

        // columns
        val put = new Put(rowKeyByte)
        put.addColumn(turnoverColFlyByte, goodsCodeByte, Bytes.toBytes(goodsNum))
        put.addColumn(turnoverColFlyByte, avgSaleNumByte, Bytes.toBytes(r.getAs[Double](avgSaleNumColName)))
        hbaseTable.mutate(put)
      })

      hbaseTable.flush()
      hbaseTable.close()
    })
    df.show()

    spSession.stop()
  }
}
