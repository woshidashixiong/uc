package user

object sql {
    
    //    --用户指标
    val user_label:String =
        """
          |SELECT
          |	od.product_id AS goods_code,  -- 商品编码
          |	SUM(od.product_num) / COUNT(DISTINCT od.user_id) AS people_avg_goods_num,  -- 人均购买商品数
          |	COUNT(od.order_id) / COUNT(DISTINCT od.user_id) AS people_avg_order_num  -- 人均购买次数
          |FROM
          |	bi_dw.fact_order_detail AS od
          |WHERE
          |	od.order_status = 1
          |	AND od.product_id != 'blg-mryx-repay'  -- 去除补款数据
          |	AND od.ptdate BETWEEN date_sub('%s', %s) AND date_sub('%s', 1)  -- ### 设定时间范围 ###
          |GROUP BY
          |	od.product_id
        """.stripMargin
}
