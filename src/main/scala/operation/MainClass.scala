package operation

import common.util


object MainClass{
    
    
    def get_wast_rate(): Unit ={
        var spark_session = util.spark_session()
        var hive_context = util.hive_context(spark_session)
        var result = hive_context.sql("SELECT id FROM bi_dw.dim_base_station limit 10")
        
        result.show()
        
    }
    
    def get_lost_rate(): Unit ={
    
    
    
    }
    

    def main (args: Array[String] ): Unit = {
        get_wast_rate()
    
    }
    
}
