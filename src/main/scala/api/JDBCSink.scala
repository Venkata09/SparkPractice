package api

/**
  * @author vdokku
  */

import java.sql._

import org.apache.spark.sql.ForeachWriter

class JDBCSink(url: String, user: String, pwd: String) extends ForeachWriter[Employee] {
  val driver = "com.mysql.jdbc.Driver"
  var connection: Connection = _
  var statement: Statement = _

  def open(partitionId: Long, version: Long): Boolean = {
    Class.forName(driver)
    connection = DriverManager.getConnection(url, user, pwd)
    statement = connection.createStatement
    true
  }

  def process(empObj: Employee): Unit = {
    statement.executeUpdate("INSERT INTO Employee_Test (user_id, job_title_name, FIRST_NAME, LAST_NAME, " +
      "preferred_full_name, employee_code, region, phone_number, email_address, formatted_name)  " +
      "VALUES (" + "'" + empObj.userId + "'" + ","
      + "'" + empObj.jobTitleName + "'" + ","
      + "'" + empObj.firstName + "'" + ","
      + "'" + empObj.lastName + "'" + ","
      + "'" + empObj.preferredFullName + "'" + ","
      + "'" + empObj.employeeCode + "'" + ","
      + "'" + empObj.region + "'" + ","
      + "'" + empObj.phoneNumber + "'" + ","
      + "'" + empObj.emailAddress + "'" + ","
      + "'" + empObj.formattedName + "'"
      + ")")
  }

  def close(errorOrNull: Throwable): Unit = {
    connection.close
  }
}