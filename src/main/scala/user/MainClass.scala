package user


import com.bjtuling.utils.DateUtil
import common.{HbasePutUtil, HbaseUtil, SessionUtil}
import org.apache.commons.lang3.math.NumberUtils
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.Logger
import enumeration.HbaseEnum.{ColumnAIUmichUmichProductLableEnum => ColumnEnum}
import enumeration.HbaseEnum.{TableAIUmichEnum => TableEnum}
import enumeration.HbaseEnum.{NamespaceEnum => DatabaseEnum}





object MainClass {
    
    private final val logger = Logger.getLogger(MainClass.getClass)
    
    
    def init(args: Array[String]):Tuple3[String, String, String] = {
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
    
        val newSql = sql.user_label.format(currentDate, dateInterval, currentDate)
        (newSql, currentDate, dateInterval)
    }
    
    
    
    def insertUserLabel2Hbase(args: Array[String]): Unit ={

    
        val (sql, current_date, date_interval) = init(args)
    
        logger.info(" current_date = " + current_date)
        logger.info(" date_interval = " + date_interval)
    
        println(" current_date = " + current_date)
        println(" date_interval = " + date_interval)
        println(" sql = " + sql)
    
        val spSession = SessionUtil.sparkSession()
        val df = spSession.sql(sql)
        df.show()
        val keyPrefix = current_date.replaceAll("-", "").reverse
        println(" keyPrefix = " + keyPrefix)
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
            val UserColFlyName = CF_USER.toString
            val turnoverColFlyByte = getColumnsBytes(UserColFlyName).get
    
            // column : goods_code
            val goodsCodeColName = GOODS_CODE.toString
            val goodsCodeByte = getColumnsBytes(goodsCodeColName).get
    
            // column : people_avg_goods_num
            val peopleAvgGoodsNum = PEOPLE_AVG_GOODS_NUM.toString
            val peopleAvgGoodsNumByte = getColumnsBytes(peopleAvgGoodsNum).get
    
            //column  : people_avg_order_num
            val peopleAvgOrderNum = PEOPLE_AVG_ORDER_NUM.toString
            val peopleAvgOrderNumByte = getColumnsBytes(peopleAvgOrderNum).get
    
            // column
            partItr.foreach(r => {
                // key
                val goodsNum = r.getAs[String](goodsCodeColName)
                val rowKeyByte = Bytes.toBytes(keyPrefixBC.value + HbasePutUtil.KEY_SEPARATOR + goodsNum)
        
                // columns
                val put = new Put(rowKeyByte)
                put.addColumn(turnoverColFlyByte, goodsCodeByte, Bytes.toBytes(goodsNum))
                put.addColumn(turnoverColFlyByte, peopleAvgOrderNumByte, Bytes.toBytes(r.getAs[Double](peopleAvgGoodsNum)))
                put.addColumn(turnoverColFlyByte, peopleAvgOrderNumByte, Bytes.toBytes(r.getAs[Double](peopleAvgOrderNum)))
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
