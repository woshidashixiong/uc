package enumeration

import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable


object HbaseEnum {

  /**
    * ai namespace
    */
  object NamespaceEnum extends Enumeration {
    type NamespaceEnum = Value
    val AIUmich = Value("ai_umich")

    private val map: mutable.HashMap[Value, Array[Byte]] = new mutable.HashMap[Value, Array[Byte]]

    NamespaceEnum.values.foreach(ns => {
      map.+=(ns -> Bytes.toBytes(ns.toString))
    })

    def getHbaseNamespaceBytes(nsName: String): Option[Array[Byte]] = {
      val ns = NamespaceEnum.values.find(_.toString == nsName)
      ns match {
        case Some(ns) => map.get(ns)
        case _ => None
      }
    }
  }

  /**
    * namespace=table_ai_umich_enum
    */
  object TableAIUmichEnum extends Enumeration {
    type TableAIUmichEnum = Value
    val UmichProductLabel = Value("umich_product_label")

    private val map: mutable.HashMap[Value, Array[Byte]] = new mutable.HashMap[Value, Array[Byte]]
    TableAIUmichEnum.values.foreach(tab => {
      map.+=(tab -> Bytes.toBytes(tab.toString))
    })

    def getHtableBytes(tabName: String): Option[Array[Byte]] = {
      val tab = values.find(_ == tabName)
      tab match {
        case Some(tab) => map.get(tab)
        case _ => None
      }
    }
  }

  /**
    * table=ai_muich.umich_product_label
    */
  object ColumnAIUmichUmichProductLableEnum extends Enumeration {
    type column_ai_umich_umich_product_lable_enum = Value
    // column_family
    val ColumnFamilyTurnover = Value("turnover")
    val ColumnFamilyOperation = Value("operation")
    val ColumnFamilyUser = Value("user")

    // columns
    val GoodsCode = Value("goods_code")

    // -- 单sku（最近一周）单点日均销量
    val GoodsDayShelfAvgSaleNum = Value("goods_day_shelf_avg_sale_num")

    // -- 单SKU（最近一周）补货数量
    val TotalReplenishmentNum = Value("total_replenishment_num")

    // -- 单SKU最近三周补货数量
    val AgvWeekRepNum = Value("avg_week_rep_num")

    // -- 单SKU（最近一周）销售进度
    val SalesProgress = Value("sales_progress")

    private val map: mutable.HashMap[Value, Array[Byte]] = new mutable.HashMap[Value, Array[Byte]]
    ColumnAIUmichUmichProductLableEnum.values.foreach(col => {
      map.+=(col -> Bytes.toBytes(col.toString))
    })

    def getColumnsBytes(colName: String): Option[Array[Byte]] = {
      val col = values.find(_.toString == colName)
      col match {
        case Some(col) => map.get(col)
        case _ => None
      }
    }
  }

  def main(args: Array[String]): Unit = {
    println(NamespaceEnum.getHbaseNamespaceBytes(NamespaceEnum.AIUmich.toString))
    println(NamespaceEnum.getHbaseNamespaceBytes("aaa"))
  }

}
