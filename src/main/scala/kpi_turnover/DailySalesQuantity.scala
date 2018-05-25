package kpi_turnover

import common.util

object DailySalesQuantity extends Serializable {

  private final val sql = s"select * from bi_dw.dim_base_station limit 10";

  def main(args: Array[String]): Unit = {
    val spSession = util.spark_session()
    val df = spSession.sql(sql)
    df.show()
  }
}
