package HandlingNullsSparkSqL

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * Created by vdokku on 2/11/2018.
  */
object HandleNulls {

  def main(args: Array[String]): Unit = {


    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")


    val spark: SparkSession = SparkSession.builder().master("local[4]").appName("<<<<<<<<<<<< ByKeyImplementation>>>>>>>>>>> ").getOrCreate()
    val sc: SparkContext = spark.sparkContext

    import spark.implicits._

    val empData = Seq(("ram", "101"), ("shaym", "102"), ("Bheem", "103"), ("Rak", "104"), ("Jack", "105"), ("Jim", "106"))
      .toDF("Emp_name", "emp_depid")


    empData.createOrReplaceTempView("emp_data")

    val depData = Seq(("103"), ("105")).toDF("DepId")

    depData.createOrReplaceTempView("dep_data")


    spark.sql("select e.Emp_name, e.emp_depid, case when d.DepId is null then e.emp_depid else d.DepId end dep_id " +
      "from " +
      "emp_data e left outer join dep_data d where e.emp_depid = d.DepId ")
      .show()


    /*
    +--------+---------+------+
|Emp_name|emp_depid|dep_id|
+--------+---------+------+
|   Bheem|      103|   103|
|    Jack|      105|   105|
+--------+---------+------+

     */


    spark.sql("select e.Emp_name, e.emp_depid, case when d.DepId is null then e.emp_depid else d.DepId end dep_id " +
      "from " +
      "emp_data e left outer join dep_data d where e.emp_depid = d.DepId ")
      .show()


    val joinedDF = empData.join(depData, $"emp_depid" === $"DepId", "left_outer")


    joinedDF.show()

  }

}
