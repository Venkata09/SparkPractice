package examples

import org.apache.spark.sql.SparkSession


/*


The DataSet and Dataframe APIs in Spark support structured analysis. An important aspect of structured analysis is the management of metadata.
These metadata may be temporary metadata (such as temporary tables), UDFs registered on SQLContext, and persistent metadata (such as Hivemeta store or HCatalog).

Earlier versions of Spark were no standard APIs to access these metadata. Users typically use query statements (such as show tables) to query these metadata.
These queries usually need to operate the original string, and different metadata types of operation is not the same.

This situation has changed in Spark 2.0. Spark 2.0 adds a standard API (called a catalog) to access the metadata in
the Spark SQL. This API can either
operate Spark SQL or manipulate Hive metadata.

　https://www.iteblog.com/archives/1701.html

 */


/**
  * Created by vdokku on 6/18/2017.
  */
object CatalogAPI {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")

    val sparkSession = SparkSession.builder().master("local[4]")
      .appName("my-spark-app")
      .getOrCreate()


    val catalog = sparkSession.catalog

    catalog.listDatabases().select("name").show(false)

    catalog.listFunctions().select("name","className","isTemporary").show(100, false)



  }
}
