package operation

object sql {
    
    //    -- 单sku标价利润率、 商品折扣率、 运营费用率
    val operation_revenue_rate:String =
        """
          |
          |SELECT
          |	od.product_id AS goods_code,  -- 商品编码
          |	(SUM(od.product_num * od.product_price) - SUM(od.product_num * od.cost_price)) / SUM(od.product_num * od.product_price)
          |		AS gross_profit_rate ,  -- 定价利润率 = (折前收入-商品成本) / 折前收入
          |	(SUM(od.product_num * od.product_price) - SUM(od.product_num * od.discount_price)) / SUM(od.product_num * od.product_price)
          |		AS discount_rate, -- 商品折扣率 = (折前收入-折后收入) / 折前收入
          |	(SUM(od.product_num * od.discount_price) - SUM(od.product_pay_price)) / SUM(od.product_num * od.product_price)
          |		AS operating_rate -- 运营费用率 = (折后收入-实收收入) / 折前收入
          |FROM
          |	bi_dw.fact_order_detail AS od
          |WHERE
          |	od.order_status = 1
          |	AND od.product_id != 'blg-mryx-repay'  -- 去除补款数据
          |	AND od.ptdate BETWEEN date_sub(%s, %s) AND date_sub(%s, 1)  -- ### 可选定时间范围 ###
          |GROUP BY
          |	od.product_id
        """.stripMargin
    
    //-- 单sku盗损占比、 报损占比
    val operation_lost_and_wast_rate:String =
        """
          |
          |SELECT
          |	A1.p_goods_code AS goods_code,  -- 商品编码
          |	-- A2.total_damage_num AS total_damage_num,  -- 总报损数量
          |	-- A2.total_lost_num AS total_lost_num,  -- 总盗损数量
          |	-- A1.total_goods_num AS total_goods_num,  -- 总在架库存
          |	A2.total_damage_num / A1.total_goods_num AS damage_proportion,  -- 报损占比
          |	A2.total_lost_num / A1.total_goods_num  AS lost_proportion  -- 盗损占比
          |FROM (
          |	-- 单sku期内（最近一周）总在架库存
          |	SELECT
          |		p.goods_code AS p_goods_code,  -- 商品编码
          |		NVL(T1.total_t0_inv,0) + NVL(T2.total_replenishment_num,0) AS total_goods_num -- 期内总在架库存
          |	FROM
          |	    bi_dw.dim_product_list AS p
          |	LEFT JOIN (
          |		-- 期初在架库存
          |		SELECT
          |			i.product_code AS i_goods_code,  -- 商品编码
          |			SUM(i.inv_0) AS total_t0_inv  -- 期初总在架库存
          |		FROM
          |			bi_dw.fact_product_inv AS i
          |		WHERE
          |			i.ptdate = date_sub(%s, %s)   -- ### 设定起始时间 ###
          |		GROUP BY
          |			i.product_code
          |		) AS T1 ON (p.goods_code = T1.i_goods_code)
          |	LEFT JOIN (
          |		-- 期内累计补货数量
          |		SELECT
          |			r.goods_code AS r_goods_code,  -- 商品编码
          |			SUM(r.receive_count) AS total_replenishment_num  -- 总补货数量
          |		FROM
          |			bi_dw.fact_erp_replenishment_data AS r
          |		WHERE
          |		    r.status = 1
          |			AND to_date(r.receive_time) BETWEEN date_sub(%s, %s) AND date_sub(%s, 1)  -- ### 设定时间范围 ###
          |		GROUP BY
          |			r.goods_code
          |		) AS T2 ON (p.goods_code = T2.r_goods_code)
          |	WHERE
          |		p.dw_end_date = '2099-12-31'
          |		AND p.product_class_name != '虚拟商品'
          |		AND NVL(T1.total_t0_inv,0) + NVL(T2.total_replenishment_num,0) != 0
          |	) AS A1
          |LEFT JOIN (
          |	-- 单sku期内（最近一周）盗损、报损数量
          |	SELECT
          |		cd.goods_no AS cd_goods_code,  -- 商品编码
          |		SUM( CASE WHEN cd.type = 1 THEN cd.count * -1 ELSE 0 END) AS total_damage_num, -- 总报损数量
          |		SUM( CASE WHEN cd.type = 2 THEN cd.count * -1 ELSE 0 END) AS total_lost_num -- 总盗损数量
          |	FROM
          |	    bi_dw.fact_inventory_check_detail_old AS cd
          |	WHERE
          |		cd.ptdate BETWEEN date_sub(%s, %s) AND date_sub(%s, 1)  -- ### 设定时间范围 ###
          |	GROUP BY
          |		cd.goods_no
          |	) AS A2 ON (A1.p_goods_code = A2.cd_goods_code)
          |
        """.stripMargin
    
}
