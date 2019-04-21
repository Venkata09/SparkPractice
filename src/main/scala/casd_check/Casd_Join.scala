package casd_check

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
//import com.datastax.spark.connector._
//import com.datastax.spark.connector.cql._
import org.apache.spark.sql.SparkSession


/**
  * @author vdokku
  */
object Casd_Join {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")


    val spark: SparkSession = SparkSession.builder().master("local[4]").appName("CityRecommendation").getOrCreate()
    val sc: SparkContext = spark.sparkContext

    import spark.implicits._

    /*val internalJoin =
      spark.sparkContext.cassandraTable("test", "table_1")
        .joinWithCassandraTable("test", "table_2")
    internalJoin.toDebugString

    internalJoin.toDF()

    res4: String = (1) CassandraJoinRDD[9] at RDD at CassandraRDD.scala:14 []
    internalJoin.collect
    internalJoin.collect.foreach(println)
    (CassandraRow{cust_id: 30, address: Japan, name: Shrela},CassandraRow{cust_id: 30, date: 2013-04-03 07:04:00+0000, product: 74F, quantity: 5})
    (CassandraRow{cust_id: 19, address: Bangalore, name: Krish},CassandraRow{cust_id: 19, date: 2013-04-03 07:03:00+0000, product: 73F, quantity: 3})
    (CassandraRow{cust_id: 14, address: Jaipur, name: Jasmine},CassandraRow{cust_id: 14, date: 2013-04-03 07:02:00+0000, product: 73F, quantity: 10})
    (CassandraRow{cust_id: 12, address: Texas, name: rita},CassandraRow{cust_id: 12, date: 2013-04-03 07:01:00+0000, product: 72F, quantity: 4})
*/

  }

}
