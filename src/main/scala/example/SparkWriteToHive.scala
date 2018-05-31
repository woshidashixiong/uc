package example

import common.SessionUtil
import org.apache.spark.sql.SaveMode

object SparkWriteToHive extends Serializable {

  private final val mysql = s"select * from bi_dw.dim_base_station limit 10";

  def main(args: Array[String]): Unit = {


    val spSession = SessionUtil.sparkSession()

    import spSession.implicits._
    import spSession.sql

    val baseStationDf = spSession.sql(mysql)
    baseStationDf.show()
    println("-----------------------------------------------------------------------")
    baseStationDf.write.mode(SaveMode.Append).insertInto("test.lp_dim_base_station")
    spSession.stop()
  }
}
