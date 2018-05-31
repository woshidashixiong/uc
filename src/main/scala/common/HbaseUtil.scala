package common

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}

/**
  * Created by lp on 2018/5/25.
  */
object HbaseUtil extends Serializable {

  /**
    * hbase conf
    *
    * @param quorum
    * @param port
    * @return
    */
  def createHbaseConfiguration(quorum: String, port: String): Configuration = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", quorum)
    conf.set("hbase.zookeeper.property.clientPort", port)
    conf
  }

  def createHbaseConfiguration(): Configuration = {
    val zookeeperHost = SessionUtil.config.getString("hbase.zookeeper.quorum")
    val zookeeperPort = SessionUtil.config.getString("hbase.zookeeper.port")
    createHbaseConfiguration(zookeeperHost, zookeeperPort)
  }

  def createHbaseConnection(): Connection = {
    val conf = createHbaseConfiguration();
    ConnectionFactory.createConnection(conf)
  }

  //创建表,需要加锁，因为spark并行调用这个方法，会出现表已经存在异常,多个executor、就会多个jvm，所以这里还是有问题
  def createTable(tableName: String, connection: Connection): Unit = synchronized {
    val admin = connection.getAdmin
    if (!admin.tableExists(TableName.valueOf(tableName))) {
      val htd = new HTableDescriptor(TableName.valueOf(tableName))
      val hcd = new HColumnDescriptor("info")
      htd.addFamily(hcd)
      admin.createTable(htd)
      admin.close()
    }
  }
}
