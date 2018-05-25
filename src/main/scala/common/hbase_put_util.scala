package common

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.{Bytes, MD5Hash}

/**
  * Created by lp on 2018/5/25.
  */
object hbase_put_util {

  /**
    * rowkey的获取
    * 如果没有id  就用create_time作为key
    *
    * @param tabMap
    * @param idMap
    * @return
    */
  @Deprecated
  def get_column_row_key(tabMap: java.util.LinkedHashMap[String, String], idMap: java.util.LinkedHashMap[String, String]): String = {
    var rkms: String = ""
    var firstKey = ""
    if (idMap.keySet().iterator().hasNext) {
      firstKey = idMap.keySet.iterator.next.toString
    }
    if (idMap.containsKey("id")) {
      rkms = idMap.get("id").toString
    } else if (firstKey.contains("id")) {
      rkms = idMap.get(firstKey).toString
    } else if (tabMap.containsKey("create_time")) {
      rkms = tabMap.get("create_time").toString
    }
    rkms
  }

  /**
    * 往hbase写数据
    *
    * @param tabMap
    * @param eventType
    * @param idMap
    * @return
    */
  def batch_par(tabMap: java.util.LinkedHashMap[String, String], eventType: String, idMap: java.util.LinkedHashMap[String, String]): Put = {
    //var rkms: String = getRkColumn(tabMap, idMap)
    var rkms: String = idMap.get("id").toString
    //将id进行reverse  散列结构
    val rowKey = rkms.reverse
    val put: Put = new Put(Bytes.toBytes(rowKey))
    try {
      val iter = tabMap.keySet().iterator()
      while (iter.hasNext) {
        val key = iter.next().toString
        val value = tabMap.get(key)
        if (value != null && value != "") {
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(key), Bytes.toBytes(value))
        }
        //  println("tabmap put a kv is : key :"+key+" value:"+ value)
      }
      val idIt = idMap.keySet().iterator()
      while (idIt.hasNext) {
        val key = idIt.next().toString
        val value = idMap.get(key)
        if (value != null && value != "") {
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(key), Bytes.toBytes(value))
        }
        // println("idmap put a kv is : key :"+key+" value:"+ value)
      }
      //  put.add(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(idMap.get("id")))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("event_class"), Bytes.toBytes(eventType))
      //      println("put a event_class is  value:"+ eventType)
    } catch {
      case ex: Exception => println("par error->>>>>" + ex.printStackTrace())
    }
    put
  }

  @Deprecated
  def put_detail(rkColumns: String, tabMap: Map[String, String]): Put = {
    var rkms: String = ""
    if (rkColumns.contains(",")) {
      val rks = rkColumns.split(",")
      for (i <- 0 to rks.length - 1) {
        val col = rks(i)
        val rkva = show_capital(tabMap.get(col))
        rkms = rkms + rkva
      }
    } else {
      rkms = tabMap.get(rkColumns).toString
    }
    val rowKey = MD5Hash.getMD5AsHex(Bytes.toBytes(rkms))
    val put: Put = new Put(Bytes.toBytes(rowKey))
    try {
      tabMap.keys.foreach(key => {
        if (tabMap.get(key) != "" || tabMap.get(key) != null) {
          val value = show_capital(tabMap.get(key))
          put.add(Bytes.toBytes("info"), Bytes.toBytes(key), Bytes.toBytes(value))
        }
      })
    } catch {
      case ex: Exception =>
    }
    put
  }


  @Deprecated
  def show_capital(x: Option[String]): String = x match {
    case Some(s) => s
    case None => "?"
  }
}
