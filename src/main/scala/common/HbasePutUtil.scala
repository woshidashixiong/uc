package common

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.{Bytes, MD5Hash}

/**
  * Created by lp on 2018/5/29.
  */
object HbasePutUtil {

  final val KeySeparator: String = ":"

  private def genRowKeyInternal(currenDate: String, id: String): Option[String] = {
    val currentDateTnal = currenDate.trim
    val idTnal = id.trim
    if (StringUtils.isEmpty(currentDateTnal) || StringUtils.isEmpty(idTnal)) {
      None
    } else {
      Some(currentDateTnal.reverse + idTnal.trim)
    }
  }

  def genRowKey(currentDate: String, id: String): Option[String] = {
    genRowKeyInternal(currentDate, id)
  }

  def genRowKeyByte(currentDate: String, id: String): Option[Array[Byte]] = {
    val opt = genRowKey(currentDate, id)
    opt match {
      case Some(s) => Some(s.getBytes(s))
      case None => None
    }
  }

  def main(args: Array[String]): Unit = {
    val sku = genRowKey("20180529", "   ")
    println(sku)
    println(genRowKeyByte("20180529", "   "))
  }
}
