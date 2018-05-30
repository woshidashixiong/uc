package enumeration

import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable


object hbase_enum {

  /**
    * ai namespace
    */
  object namespace_enum extends Enumeration {
    type namespace_enum = Value
    val ai_umich = Value("ai_umich")

    private val map: mutable.HashMap[Value, Array[Byte]] = new mutable.HashMap[Value, Array[Byte]]

    namespace_enum.values.foreach(ns => {
      map.+=(ns -> Bytes.toBytes(ns.toString))
    })

    def get_hbase_namespace_bytes(ns_name: String): Option[Array[Byte]] = {
      val ns = namespace_enum.values.find(_.toString == ns_name)
      ns match {
        case Some(ns) => map.get(ns)
        case _ => None
      }
    }
  }

  /**
    * namespace=table_ai_umich_enum
    */
  object table_ai_umich_enum extends Enumeration {
    type table_ai_umich_enum = Value
    val umich_product_label = Value("umich_product_label")

    private val map: mutable.HashMap[Value, Array[Byte]] = new mutable.HashMap[Value, Array[Byte]]
    table_ai_umich_enum.values.foreach(tab => {
      map.+=(tab -> Bytes.toBytes(tab.toString))
    })

    def get_htable_bytes(tab_name: String): Option[Array[Byte]] = {
      val tab = values.find(_ == tab_name)
      tab match {
        case Some(tab) => map.get(tab)
        case _ => None
      }
    }
  }

  /**
    * table=ai_muich.umich_product_label
    */
  object column_ai_umich_umich_product_lable_enum extends Enumeration {
    type column_ai_umich_umich_product_lable_enum = Value
    // column_family
    val column_family_turnover = Value("turnover")
    val column_family_operation = Value("operation")
    val column_family_user = Value("user")

    // columns
    val goods_code = Value("goods_code")
    val goods_day_shelf_avg_sale_num = Value("goods_day_shelf_avg_sale_num")

    private val map: mutable.HashMap[Value, Array[Byte]] = new mutable.HashMap[Value, Array[Byte]]
    column_ai_umich_umich_product_lable_enum.values.foreach(col => {
      map.+=(col -> Bytes.toBytes(col.toString))
    })

    def get_columns_bytes(col_name: String): Option[Array[Byte]] = {
      val col = values.find(_.toString == col_name)
      col match {
        case Some(col) => map.get(col)
        case _ => None
      }
    }
  }

  def main(args: Array[String]): Unit = {
    println(namespace_enum.get_hbase_namespace_bytes(namespace_enum.ai_umich.toString))
    println(namespace_enum.get_hbase_namespace_bytes("aaa"))
  }

}
