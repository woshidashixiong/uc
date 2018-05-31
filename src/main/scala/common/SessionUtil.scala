package common

import java.io.{File, FileInputStream}
import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext

object SessionUtil {

  lazy val config: Config = ConfigFactory.load()

  // initial spark context
  def sparkContext(): SparkContext = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "prod_selection")
    sc
  }

  // load data from disk
  def loadDataFromDisk(sc: SparkContext, path: String): RDD[String] = {
    val data = sc.textFile(path)
    data
  }

  // load property config file
  def loadConfig(): Properties = {
    val pwd = new File(".").getCanonicalPath()
    val path_file = "/src/main/resources/application.conf"
    val path = pwd + path_file
    println(" path = " + path)
    val inputStream = new FileInputStream(path)
    val config = new Properties()
    config.load(inputStream)
    config
  }

  // load config file
  def getConfig(variable: String): String = {
    loadConfig().getProperty(variable)
  }

  // load spark session
  def sparkSession(): SparkSession = {
    val spSession = SparkSession
      .builder()
      //.master("local[1]")
      //.appName("hivemodes")
      //.config("spark.defalut.parallelism", config.getString("partitions.num"))
      .config("spark.rdd.compress", "true")
      .config("spark.locality.wait", "120")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", "1024")
      .config("spark.sql.inMemoryColumnarStorage.batchSize", "10000")
      .config("hive.metastore.uris", config.getString("hive.url"))
      .enableHiveSupport()
      .getOrCreate()
    spSession
  }

  // return spark context
  def sparkContextCluster(ss: SparkSession): SparkContext = {
    ss.sparkContext
  }

  // load hive context
  def hiveContext(ss: SparkSession): HiveContext = {

    val hiveContext = new HiveContext(ss.sparkContext)
    hiveContext
  }

}
