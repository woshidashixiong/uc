package kpi_revenue

import common.{hbase_util, util}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes


object MainClass{
    
    
    private final val table_wast_and_lost_rate:String = "dw_test:wast_and_lost_rate"
    private final val table_sku_price_and_discount_and_operate_rate:String = "dw_test:sku_price_and_discount_and_operate_rate"
    private final val tableColumnFamily = "info"
    
    
    def handle_wast_and_lost_rate(): Unit ={
        val spark_session = util.spark_session()
        val hive_context = util.hive_context(spark_session)
        val wast_rate_result = hive_context.sql(sql.revenue_lost_and_wast_rate)
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
            val wastProportionByte = Bytes.toBytes("wast_proportion")
            val lostProportion = Bytes.toBytes("lost_proportion")

    
            partItr.foreach(r => {
                val rowKeyByte = Bytes.toBytes(r.getAs[Int]("goods_code"))
                val put = new Put(rowKeyByte)
                put.addColumn(columnFamilyByte, goodsCodeByte, Bytes.toBytes(r.getAs[String]("goods_code")))
                put.addColumn(columnFamilyByte, wastProportionByte, Bytes.toBytes(r.getAs[Double]("damage_proportion").toString))
                put.addColumn(columnFamilyByte, lostProportion, Bytes.toBytes(r.getAs[Double]("lost_proportion").toString))

                m.mutate(put)
            })
    
            m.flush()
            m.close()
        })
        
    }
    
    
    def handle_sku_price_and_discount_and_operate_rate(): Unit ={
        val spark_session = util.spark_session()
        val hive_context = util.hive_context(spark_session)
        val results = hive_context.sql(sql.revenue_rate)
        results.show()
    
        results.foreachPartition(partItr => {
            // connection
            val conn = hbase_util.create_hbase_connection()
        
            // table
            val hbaseTableName = TableName.valueOf(table_sku_price_and_discount_and_operate_rate)
            val m = conn.getBufferedMutator(hbaseTableName)
        
            // column
            val columnFamilyByte = Bytes.toBytes(tableColumnFamily)
            val goodsCodeByte = Bytes.toBytes("goods_code")
            val grossProfitRateByte = Bytes.toBytes("gross_profit_rate")
            val discountRateByte = Bytes.toBytes("discount_rate")
            val operatingRateByte = Bytes.toBytes("operating_rate")
        
        
            partItr.foreach(r => {
                val rowKeyByte = Bytes.toBytes(r.getAs[String]("goods_code"))
                val put = new Put(rowKeyByte)
                put.addColumn(columnFamilyByte, goodsCodeByte, Bytes.toBytes(r.getAs[String]("goods_code")))
                put.addColumn(columnFamilyByte, grossProfitRateByte, Bytes.toBytes(r.getAs[Double]("gross_profit_rate").toString))
                put.addColumn(columnFamilyByte, discountRateByte, Bytes.toBytes(r.getAs[Double]("discount_rate").toString))
                put.addColumn(columnFamilyByte, operatingRateByte, Bytes.toBytes(r.getAs[Double]("operating_rate").toString))
            
                m.mutate(put)
            })
        
            m.flush()
            m.close()
        })
        
        
    }
    

    def main (args: Array[String] ): Unit = {
        handle_sku_price_and_discount_and_operate_rate()
//        handle_wast_and_lost_rate()
    
    }
    
}
