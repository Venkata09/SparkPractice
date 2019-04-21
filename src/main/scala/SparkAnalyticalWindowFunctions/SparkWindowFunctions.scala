package SparkAnalyticalWindowFunctions

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window

object SparkWindowFunctions {


  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master")

    val spark: SparkSession = SparkSession.builder().master("local[4]")
      .appName("QueryingWithStreamingSources").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    import spark.implicits._


    // Create Sample Dataframe
    val empDF = spark.createDataFrame(Seq(
      (7369, "SMITH", "CLERK", 7902, "17-Dec-80", 800, 20, 10),
      (7499, "ALLEN", "SALESMAN", 7698, "20-Feb-81", 1600, 300, 30),
      (7521, "WARD", "SALESMAN", 7698, "22-Feb-81", 1250, 500, 30),
      (7566, "JONES", "MANAGER", 7839, "2-Apr-81", 2975, 0, 20),
      (7654, "MARTIN", "SALESMAN", 7698, "28-Sep-81", 1250, 1400, 30),
      (7698, "BLAKE", "MANAGER", 7839, "1-May-81", 2850, 0, 30),
      (7782, "CLARK", "MANAGER", 7839, "9-Jun-81", 2450, 0, 10),
      (7788, "SCOTT", "ANALYST", 7566, "19-Apr-87", 3000, 0, 20),
      (7839, "KING", "PRESIDENT", 0, "17-Nov-81", 5000, 0, 10),
      (7844, "TURNER", "SALESMAN", 7698, "8-Sep-81", 1500, 0, 30),
      (7876, "ADAMS", "CLERK", 7788, "23-May-87", 1100, 0, 20)
    )).toDF("empno", "ename", "job", "mgr", "hiredate", "sal", "comm", "deptno")

    val partitionWindow = Window.partitionBy($"deptno").orderBy($"sal".desc)

    empDF.createOrReplaceTempView("emp")

    spark.sql("" +
      "SELECT empno,deptno,sal,RANK() OVER (partition by deptno ORDER BY sal desc) as rank FROM emp ").show()

    import org.apache.spark.sql.functions._

    val rankTest = rank().over(partitionWindow)
    empDF.select($"*", rankTest as "rank").show


    println("--------------------- DENSE RANK ---------------------")

    spark.sql("" +
      "SELECT empno,deptno,sal,DENSE_RANK() OVER (PARTITION BY deptno ORDER BY sal desc) as dense_rank FROM emp ").show()


    val denseRankTest = rank().over(partitionWindow)
    empDF.select($"*", denseRankTest as "rank").show

    println("--------------------- Row Number ---------------------")

    spark.sql("" +
      "SELECT empno,deptno,sal,ROW_NUMBER() OVER (PARTITION BY deptno ORDER BY sal desc) as row_num FROM emp ").show()


    val rowNumberTest = row_number().over(partitionWindow)
    empDF.select($"*", rowNumberTest as "row_number").show

    println("--------------------- Running Total (Salary) within each department ---------------------")
    spark.sql("" +
      " SELECT empno,deptno,sal,sum(sal) OVER (PARTITION BY deptno ORDER BY sal desc) as running_total FROM emp ").show()


    val sumTest = sum($"sal").over(partitionWindow)
    empDF.select($"*", sumTest as "running_total").show


    println("--------------------- Lead function allows us to compare current row with subsequent rows within each partition depending on the second argument (offset) which is by default set to 1 i.e. next row but you can change that parameter 2 to compare against every other row.The 3rd parameter is default value to be returned when no subsequent values exists or nul ---------------------")
    spark.sql("" +
      " SELECT empno,deptno,sal,lead(sal) OVER (PARTITION BY deptno ORDER BY sal desc) as next_val FROM emp ").show()


    val leadTest = lead($"sal", 1, 0).over(partitionWindow)
    empDF.select($"*", leadTest as "next_val").show

    println("--------------------- Lag function allows us to compare current row with preceding rows within each partition depending on the second argument (offset) which is by default set to 1 i.e. previous row but you can change that parameter 2 to compare against every other preceding row.The 3rd parameter is default value to be returned when no preceding values exists or null. ---------------------")
    spark.sql("" +
      " SELECT empno,deptno,sal,lag(sal) OVER (PARTITION BY deptno ORDER BY sal desc) as pre_val FROM emp ").show()


    val lagTest = lag($"sal", 1, 0).over(partitionWindow)
    empDF.select($"*", lagTest as "prev_val").show


    println("--------------------- First value within each partition .i.e. highest salary (we are using order by descending) within each department can be compared against every member within each department. ---------------------")
    spark.sql("" +
      " SELECT empno,deptno,sal,first_value(sal) OVER (PARTITION BY deptno ORDER BY sal desc) as first_val FROM emp ").show()


    val firstValTest = first($"sal").over(partitionWindow)
    empDF.select($"*", firstValTest as "first_val").show


    println("--------------------- Last value within each partition .i.e. lowet salary (we are using order by descending) within each department can be compared against every member within each department.. ---------------------")
    spark.sql("" +
      " SELECT empno,deptno,sal,last_value(sal) OVER (PARTITION BY deptno ORDER BY sal desc) as last_val FROM emp ").show()


    val lastValTest = last($"sal").over(partitionWindow)
    empDF.select($"*", lastValTest as "last_val").show



    /*
    Oops what happened here the last_val has the same value as in sal column but we were expecting the lowest salary within the department in the last_val column so for that we really need to understand how the window operates and works. There are two types of frames ROW and RANGE.The details are explained in this posts from databricks.

    This happens because default window frame is range between unbounded preceding and current row, so the last_value() never looks beyond current row unless you change the frame.


    //Define new window partition to operate on row frame
    val partitionWindowWithUnboundedFollowing = Window.partitionBy($"deptno").orderBy($"sal".desc).rowsBetween(Window.currentRow, Window.unboundedFollowing)

     */

    println("--------------------- Last value fixed by supplying the window frame for last_val() to operate on. We will be using start frame current row and end frame unbounded following to get the last value.. ---------------------")
    spark.sql("" +
      " SELECT empno,deptno,sal,last_value(sal) OVER (PARTITION BY deptno ORDER BY sal desc " +
      "ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) as last_val FROM emp; ").show()

    val partitionWindowWithUnboundedFollowing = Window.partitionBy($"deptno").orderBy($"sal".desc).rowsBetween(Window.currentRow, Window.unboundedFollowing)


    val lastValTest2 = last($"sal").over(partitionWindowWithUnboundedFollowing)
    empDF.select($"*", lastValTest2 as "last_val").show


  }

}
