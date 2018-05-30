package turnover

import common.util

/**
  * Created by lipeng
  * 2018-05-29
  * <br/>
  * DMS-DAILY MEAN SALES
  */
object DailyMeanSales extends Serializable {

  private final val sql =
    s"""SELECT
      T1.sp_goods_code AS goods_code,  -- 商品编码
      -- nvl(T2.total_sale_num, 0) AS total_sale_num, -- 总销量
	    -- T1.total_sale_days AS total_sale_days, -- 累计运营天数
	    nvl(T2.total_sale_num, 0) / T1.total_sale_days AS goods_day_shelf_avg_sale_num -- 单点日均销量 = 总销量 / 累计运营天数
    FROM (
      -- 单SKU期内累计运营天数
      SELECT
        sp.goods_code AS sp_goods_code,  -- 商品编码
        COUNT(DISTINCT CONCAT(sp.goods_shelf_code, sp.goods_code, sp.ptdate)) AS total_sale_days  -- 累计运营天数
      FROM
        bi_dw.dim_shelf_product_his AS sp
      WHERE
        sp.status = 1
        AND sp.product_status = 1
        AND sp.shop_code IN (
          SELECT
            shop.shop_code
          FROM
            bi_dw.dim_shop_list AS shop
          WHERE
            shop.city != '540200'
            AND shop.dw_end_date = '2099-12-31')
          AND sp.product_class_name != '虚拟商品'
        AND sp.ptdate BETWEEN date_sub(current_date, 7) AND date_sub(current_date, 1)  -- ### 设定时间范围 ###
      GROUP BY
        sp.goods_code
      ) AS T1
    LEFT JOIN (
      -- 单sku期内总销量
      SELECT
        od.product_id AS od_goods_code,  -- 商品编码
        SUM(od.product_num) AS total_sale_num  -- 总销量
      FROM
        bi_dw.fact_order_detail AS od
        JOIN bi_dw.dim_product_list AS p ON (p.goods_code = od.product_id AND p.product_class_name != '虚拟商品' AND p.dw_end_date = '2099-12-31')
      WHERE
        od.order_status = 1
        AND od.ptdate BETWEEN date_sub(current_date, 7) AND date_sub(current_date, 1)  -- ### 设定时间范围 ###
      GROUP BY
        od.product_id
      )  AS T2 ON (T1.sp_goods_code = T2.od_goods_code)""".stripMargin;

  def main(args: Array[String]): Unit = {
    val spSession = util.spark_session()
    val df = spSession.sql(sql).limit(10)
    df.show()
  }
}
