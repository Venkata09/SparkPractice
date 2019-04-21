package api

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * Created by vdokku on 1/29/2018.
  */
object FoldByKeyExample {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")


    val spark: SparkSession = SparkSession.builder().master("local[4]").appName("<<<<<<<<<<<< ByKeyImplementation>>>>>>>>>>> ").getOrCreate()
    val sc: SparkContext = spark.sparkContext


    //Fold in spark
    val employeeData = List(("Jack", 1000.0), ("Bob", 2000.0), ("Carl", 7000.0))
    val employeeRDD = sc.makeRDD(employeeData)

    val dummyEmployee = ("dummy", 0.0)

    val maxSalaryEmployee = employeeRDD.fold(dummyEmployee)((acc, employee) => {
      if (acc._2 < employee._2) employee else acc
    })
    println("employee with maximum salary is" + maxSalaryEmployee)


    //Fold by key
    val deptEmployees = List(
      ("cs", ("jack", 1000.0)),
      ("cs", ("bron", 1200.0)),
      ("phy", ("sam", 2200.0)),
      ("phy", ("ronaldo", 500.0)))

    val empRDD = sc.makeRDD(deptEmployees)

    val dummyEmp = ("dummy", 0.0)

    val maxByDept = empRDD.foldByKey(dummyEmp)(
      (acc, employee) => {
        if (acc._2 < employee._2) employee else acc
      }
    )
    println("maximum salaries in each dept" + maxByDept.collect().toList)

    //Fold by key
    var rdd1 = sc.makeRDD(Array(("A", 0), ("A", 2), ("B", 1), ("B", 2), ("C", 1)))
    rdd1.foldByKey(0)(_ + _).collect.foreach(f => println(f))
    println("-----------------------------------------------")
    rdd1.foldByKey(1)(_ * _).collect.foreach(f => println(f))


  }

}
