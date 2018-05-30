package common

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}

/**
  * Created by lp on 2018/5/25.
  */
object hbase_util extends Serializable {

  final val column_key_separator:String = ":"

  /**
    * hbase conf
    *
    * @param quorum
    * @param port
    * @return
    */
  def create_hbase_configuration(quorum: String, port: String): Configuration = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", quorum)
    conf.set("hbase.zookeeper.property.clientPort", port)
    conf
  }

  def create_hbase_configuration(): Configuration = {
    val zookeeperHost = util.config.getString("hbase.zookeeper.quorum")
    val zookeeperPort = util.config.getString("hbase.zookeeper.port")
    create_hbase_configuration(zookeeperHost, zookeeperPort)
  }

  def create_hbase_connection(): Connection = {
    val conf = create_hbase_configuration();
    val conn = ConnectionFactory.createConnection(conf)
    conn
  }

  //创建表,需要加锁，因为spark并行调用这个方法，会出现表已经存在异常,多个executor、就会多个jvm，所以这里还是有问题
  def create_table(table_name: String, connection: Connection): Unit = synchronized {
    val admin = connection.getAdmin
    if (!admin.tableExists(TableName.valueOf(table_name))) {
      val htd = new HTableDescriptor(TableName.valueOf(table_name))
      val hcd = new HColumnDescriptor("info")
      htd.addFamily(hcd)
      admin.createTable(htd)
      admin.close()
    }
  }
}
