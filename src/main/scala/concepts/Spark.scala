package concepts

import org.apache.spark.sql.SparkSession

trait Spark {

  def createSession(local: Boolean): SparkSession = {
    val builder = SparkSession.builder().appName(this.getClass.getSimpleName)


    builder.getOrCreate()

  }
}
