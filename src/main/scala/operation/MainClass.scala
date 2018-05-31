package operation

import common.SessionUtil


object MainClass {


  def getWastRate(): Unit = {
    var spSession = SessionUtil.sparkSession()
    var hiveContext = SessionUtil.hiveContext(spSession)
    var result = hiveContext.sql("SELECT id FROM bi_dw.dim_base_station limit 10")
    result.show()
  }

  def getLostRate(): Unit = {


  }


  def main(args: Array[String]): Unit = {
    getWastRate()

  }

}
