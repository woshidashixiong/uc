package common

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.{Bytes, MD5Hash}

/**
  * Created by lp on 2018/5/29.
  */
object hbase_put_util {

  private def gen_row_key_internal(current_date: String, id: String): Option[String] = {
    val current_date_tnal = current_date.trim
    val id_tnal = id.trim
    if (StringUtils.isEmpty(current_date_tnal) || StringUtils.isEmpty(id_tnal)) {
      None
    } else {
      Some(current_date_tnal.reverse + id_tnal.trim)
    }
  }

  def gen_row_key(current_date: String, id: String): Option[String] = {
    gen_row_key_internal(current_date, id)
  }

  def gen_row_key_byte(current_date: String, id: String): Option[Array[Byte]] = {
    val opt = gen_row_key(current_date, id)
    opt match {
      case Some(s) => Some(s.getBytes(s))
      case None => None
    }
  }

  def main(args: Array[String]): Unit = {
    val sku = gen_row_key("20180529", "   ")
    println(sku)
    println(gen_row_key_byte("20180529", "   "))
  }
}
