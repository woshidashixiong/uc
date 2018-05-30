package turnover

import com.bjtuling.utils.date_util
import common.{hbase_util, util}
import enumeration.hbase_enum.{column_ai_umich_umich_product_lable_enum => column_enum}
import enumeration.hbase_enum.{table_ai_umich_enum => table_enum}
import enumeration.hbase_enum.{namespace_enum => database_enum}
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

    var date_interval = "7"
    var current_date = ""
    if (null != args && !args.isEmpty) {
      if (NumberUtils.isDigits(args(0).trim)) {
        logger.error(" The interval of data is not a number !")
        System.exit(-1)
      }
      date_interval = args(0).trim

      if (args.length >= 2) {
        current_date = args(1)
      }
    } else {
      current_date = date_util.get_yesterday()
      logger.warn(" The argument about data_interval and current_date are both null.")
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
          |		  AND sp.ptdate BETWEEN date_sub('${current_date}', ${date_interval}) AND date_sub('${current_date}', 1)
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
          |		  AND od.ptdate BETWEEN date_sub('${current_date}', ${date_interval}) AND date_sub('${current_date}', 1)
          |		GROUP BY od.product_id
          |	)  AS T2
          |	ON (T1.sp_goods_code = T2.od_goods_code)""".stripMargin;

    (sql, current_date, date_interval)
  }

  def main(args: Array[String]): Unit = {

    val (sql, current_date, date_interval) = init(args)

    logger.info(" current_date = " + current_date)
    logger.info(" date_interval = " + date_interval)

    println(" current_date = " + current_date)
    println(" date_interval = " + date_interval)
    println(" sql = " + sql)

    val spSession = util.spark_session()
    println("11111111")
    val df = spSession.sql(sql)
    val keyPrefix = current_date.replaceAll("-", "").reverse
    println(" keyPrefix = " + keyPrefix)
    val keyPrefixBC = spSession.sparkContext.broadcast(keyPrefix)

    df.foreachPartition(partItr => {
      // connection
      val conn = hbase_util.create_hbase_connection()

      // table
      import table_enum._
      import database_enum._
      val hbaseTableName = TableName.valueOf(ai_umich.toString + ":" + umich_product_label.toString)
      val hbaseTable = conn.getBufferedMutator(hbaseTableName)

      // column-family : turnover
      import column_enum._
      val turnoverColFlyName = column_family_turnover.toString
      val turnoverColFlyByte = get_columns_bytes(turnoverColFlyName).get

      // column : goods_code
      val goodsCodeColName = goods_code.toString
      val goodsCodeByte = get_columns_bytes(goodsCodeColName).get

      // column : goods_day_shelf_avg_sale_num
      val avgSaleNumColName = goods_day_shelf_avg_sale_num.toString
      val avgSaleNumByte = get_columns_bytes(avgSaleNumColName).get

      // column
      partItr.foreach(r => {
        // key
        val goodsNum = r.getAs[String](goodsCodeColName)
        val rowKeyByte = Bytes.toBytes(keyPrefixBC.value + hbase_util.column_key_separator + goodsNum)

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
