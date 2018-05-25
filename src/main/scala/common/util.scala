package common

import java.io.{File, FileInputStream}
import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext

object util {
  // initial spark context
  def spark_context(): SparkContext = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "prod_selection")
    sc
  }

  // load data from disk
  def load_data_from_disk(sc: SparkContext, path: String): RDD[String] = {
    val data = sc.textFile(path)
    data
  }

  // load property config file
  def load_config(): Properties = {
    val pwd = new File(".").getCanonicalPath()
    val path_file = "/src/main/resources/application.properties"
    val path = pwd + path_file
    println(" path = " + path)
    val inputStream = new FileInputStream(path)
    val config = new Properties()
    config.load(inputStream)
    config
  }

  // load config file
  def get_config(variable: String): String = {
    load_config().getProperty(variable)
  }

  // load spark session
  def spark_session(): SparkSession = {
    val config: Config = ConfigFactory.load()

    val spSession = SparkSession
      .builder()
      .master("local[1]")
      .appName("hivemodes")
      .config("spark.defalut.parallelism", config.getString("partitions.num"))
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
  def spark_context_cluster(ss: SparkSession): SparkContext = {
    ss.sparkContext
  }

  // load hive context
  def hive_context(ss: SparkSession): HiveContext = {

    val hiveContext = new HiveContext(ss.sparkContext)
    hiveContext
  }

}
