package Spark_2_2_Learning_Notes

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession


/*
https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/2728434780191932/1483312212640900/6987336228780374/latest.html
https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/2728434780191932/1483312212640900/6987336228780374/latest.html



https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/2728434780191932/1483312212640900/6987336228780374/latest.html




 */

object Spark_SubQueries {

  def main(args: Array[String]): Unit = {


    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")


    val spark: SparkSession = SparkSession.builder().master("local[4]")
      .appName("<<<<<<<<<<<< ByKeyImplementation>>>>>>>>>>> ").getOrCreate()


    import spark.implicits._
    import org.apache.spark.sql.functions._


    import org.apache.spark.sql.functions._
    val employee = spark.range(0, 10).select($"id".as("employee_id"), (rand() * 3).cast("int").as("dep_id"), (rand() * 40 + 20).cast("int").as("age"))
    val visit = spark.range(0, 100).select($"id".as("visit_id"), when(rand() < 0.95, ($"id" % 8)).as("employee_id"))
    val appointment = spark.range(0, 100).select($"id".as("appointment_id"), when(rand() < 0.95, ($"id" % 7)).as("employee_id"))
    employee.createOrReplaceTempView("employee")
    visit.createOrReplaceTempView("visit")
    appointment.createOrReplaceTempView("appointment")

    /*
    Setup
Lets create a simple data model with some random data before we get started.
The model contains the following tables:

Employee: this table contains employee information (id, department and age).

employee_id, depart_id, age
visit_id, employee_id
appointment_id,employee_id


scalar and predicate subqueries.



Scalar Subqueries
Scalar sub-queries are sub-queries that return a single result.
Scalar sub-queries can either be correlated or uncorrelated.

Uncorrelated Scalar Subqueries
An uncorrelated subquery returns the same single value for all records in a
query.
Uncorrelated sub-queries are executed by the Spark engine before the main
query is executed.

The SQL below shows an example of an uncorrelated scalar subquery,
here we add the maximum age in table employee to the select.


     */


spark.sql("select employee_id, age, " +
  "(select MAX(age) from employee) max_age from employee").show()


/*
// TODO: Correlated Scalar Sub-queries
Subqueries can be correlated,
this means that the subquery contains references to the outer query.
These outer references are typically used in filter clauses (SQL WHERE clause).
Spark 2.0 currently only supports this case.
The SQL below shows an example of a correlated scalar subquery,
here we add the maximum age in an employee’s department to the select list
using A.dep_id = B.dep_id as the correlated condition.


 */

    spark.sql("select A.employee_id, A.age, A.dep_id, " +
      "(select MAX(age) from employee B where A.dep_id = B.dep_id) max_age " +
      "from employee A order by 1,2").show()


    /*
    So what is going on under-neath
     */




    spark.sql("select A.employee_id, A.dep_id, A.age, B.max_age from employee A " +
      " left outer join (select dep_id, MAX(age) max_age from employee B group by dep_id) B ON B.dep_id = A.dep_id order by 1, 2").show()






  }

}
