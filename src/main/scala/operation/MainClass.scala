package operation

import com.bjtuling.utils.DateUtil
import common.{HbasePutUtil, HbaseUtil, SessionUtil}
import enumeration.HbaseEnum.ColumnAIUmichUmichProductLableEnum._

import org.apache.commons.lang3.math.NumberUtils
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.Logger
import enumeration.HbaseEnum.{TableAIUmichEnum => TableEnum}
import enumeration.HbaseEnum.{NamespaceEnum => DatabaseEnum}
import enumeration.HbaseEnum.{ColumnAIUmichUmichProductLableEnum => ColumnEnum}


object MainClass {


  private final val logger = Logger.getLogger(MainClass.getClass)


  def init(args: Array[String]): Tuple4[String, String, String, String] = {
    var dateInterval = "7"
    var currentDate = ""
    if (null != args && !args.isEmpty) {
      if (NumberUtils.isDigits(args(0).trim)) {
        logger.error(" The interval of data is not a number !")
        System.exit(-1)
      }
      dateInterval = args(0).trim

      if (args.length >= 2) {
        currentDate = args(1)
      }
    } else {
      currentDate = DateUtil.getYesterday()
      logger.warn(" The argument about dateInterval and currentDate are both null.")
    }

    val newSql1 = sql.operation_revenue_rate.format(currentDate, dateInterval, currentDate)
    val newSql2 = sql.operation_lost_and_wast_rate.format(currentDate, dateInterval, currentDate, dateInterval, currentDate, currentDate, dateInterval, currentDate)
    (newSql1, newSql1, currentDate, dateInterval)
  }


  def insertUserLabel2Hbase(args: Array[String]): Unit = {


    val (sql1, sql2, current_date, date_interval) = init(args)

    logger.info(" current_date = " + current_date)
    logger.info(" date_interval = " + date_interval)

    println(" current_date = " + current_date)
    println(" date_interval = " + date_interval)
    println(" sql = " + sql1)
    println(" sql = " + sql2)

    val spSession = SessionUtil.sparkSession()
    val df1 = spSession.sql(sql1)
    df1.show()

    val df2 = spSession.sql(sql1)
    df2.show()

    val df = df1.join(df2)


    val keyPrefix = current_date.replaceAll("-", "").reverse
    println("keyPrefix = " + keyPrefix)
    val keyPrefixBC = spSession.sparkContext.broadcast(keyPrefix)

    df.foreachPartition(partItr => {
      // connection
      val conn = HbaseUtil.createHbaseConnection()

      // table
      import TableEnum._
      import DatabaseEnum._
      val hbaseTableName = TableName.valueOf(AI_UMICH.toString + ":" + UMICH_PRODUCT_LABEL.toString)
      val hbaseTable = conn.getBufferedMutator(hbaseTableName)

      // column-family : user
      import ColumnEnum._
      val OperationColFlyName = CF_OPERATION.toString
      val turnoverColFlyByte = getColumnsBytes(OperationColFlyName).get

      // column : goods_code
      val goodsCodeColName = GOODS_CODE.toString
      val goodsCodeByte = getColumnsBytes(goodsCodeColName).get
      //----------------------------------------------------------
      // column : gross_profit_rate
      val grossProfitRateNum = GROSS_PROFIT_RATE_NUM.toString
      val grossProfitRateNumByte = getColumnsBytes(grossProfitRateNum).get

      //column  : discount_rate
      val discountRateNum = DISCOUNT_RATE_NUM.toString
      val discountRateNumByte = getColumnsBytes(discountRateNum).get

      // column : operating_rate
      val operatingRateNum = OPERATION_RATE_NUM.toString
      val operatingRateNumByte = getColumnsBytes(operatingRateNum).get

      //column  : damage_proportion
      val damageProportionNum = DAMAGE_PROORTION_NUM.toString
      val damageProportionNumByte = getColumnsBytes(damageProportionNum).get

      // column : lost_proportion
      val lostProportionNumNum = LOST_PROPORTION_NUM.toString
      val lostProportionNumNumByte = getColumnsBytes(lostProportionNumNum).get


      // column
      partItr.foreach(r => {
        // key
        val goodsNum = r.getAs[String](goodsCodeColName)
        val rowKeyByte = Bytes.toBytes(keyPrefixBC.value + HbasePutUtil.KEY_SEPARATOR + goodsNum)

        // columns
        val put = new Put(rowKeyByte)
        put.addColumn(turnoverColFlyByte, goodsCodeByte, Bytes.toBytes(goodsNum))
        put.addColumn(turnoverColFlyByte, grossProfitRateNumByte, Bytes.toBytes(r.getAs[Double](grossProfitRateNum)))
        put.addColumn(turnoverColFlyByte, discountRateNumByte, Bytes.toBytes(r.getAs[Double](discountRateNum)))
        put.addColumn(turnoverColFlyByte, operatingRateNumByte, Bytes.toBytes(r.getAs[Double](operatingRateNum)))
        put.addColumn(turnoverColFlyByte, damageProportionNumByte, Bytes.toBytes(r.getAs[Double](damageProportionNum)))
        put.addColumn(turnoverColFlyByte, lostProportionNumNumByte, Bytes.toBytes(r.getAs[Double](lostProportionNumNum)))
        hbaseTable.mutate(put)
      })

      hbaseTable.flush()
      hbaseTable.close()
    })
    df.show()

    spSession.stop()
  }

  def main(args: Array[String]): Unit = {
    insertUserLabel2Hbase(args)
  }

}
