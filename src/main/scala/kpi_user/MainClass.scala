package kpi_user

import common.{hbase_util, util}
import kpi_revenue.MainClass.{tableColumnFamily, table_wast_and_lost_rate}
import kpi_user.sql
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

object MainClass {
    
    private final val table_wast_and_lost_rate:String = "dw_test:user_rate"
    private final val tableColumnFamily = "info"
    
    def get_user_avg_buy_num(): Unit ={
        val spark_session = util.spark_session()
        val hive_context = util.hive_context(spark_session)
        val wast_rate_result = hive_context.sql(sql.user_avg_buy_num)
        wast_rate_result.show()
    
        wast_rate_result.foreachPartition(partItr => {
            // connection
            val conn = hbase_util.create_hbase_connection()
        
            // table
            val hbaseTableName = TableName.valueOf(table_wast_and_lost_rate)
            val m = conn.getBufferedMutator(hbaseTableName)
        
            // column
            val columnFamilyByte = Bytes.toBytes(tableColumnFamily)
            val goodsCodeByte = Bytes.toBytes("goods_code")
            val peopleAvgGoodsNumByte = Bytes.toBytes("people_avg_goods_num")
            
            partItr.foreach(r => {
                val rowKeyByte = Bytes.toBytes(r.getAs[Int]("goods_code"))
                val put = new Put(rowKeyByte)
                put.addColumn(columnFamilyByte, goodsCodeByte, Bytes.toBytes(r.getAs[String]("goods_code")))
                put.addColumn(columnFamilyByte, peopleAvgGoodsNumByte, Bytes.toBytes(r.getAs[Double]("people_avg_goods_num").toString))
                
                m.mutate(put)
            })
        
            m.flush()
            m.close()
        })
        
    }
    
    
    def get_user_avg_buy_time(): Unit ={
        val spark_session = util.spark_session()
        val hive_context = util.hive_context(spark_session)
        val lost_rate_result = hive_context.sql(sql.user_avg_buy_time)
        lost_rate_result.show()
    }
    
    
    def main (args: Array[String] ): Unit = {
        get_user_avg_buy_num()
        get_user_avg_buy_time()
        
    }
    
}
