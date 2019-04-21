package movies_analysis

import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

object Driver {
  def main(args: Array[String]): Unit = {

   /* require(args.length == 2,
      "You should provide the movies.dat location,the ratings.dat location and the output path")*/
    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")
    val sparkSession = SparkSession.builder.
      master("local")
      .appName("example")
      .getOrCreate()

    val sparkContext = sparkSession.sparkContext

    Try {
      Job.run("src/main/resources/movies_DB/movies.dat", "src/main/resources/movies_DB/ratings.dat")
    } match {
      case Success(_) => println("Success starting the job")
      case Failure(ex) => println(s"The job failed starting: [${ex.getMessage}]")
    }

  }
}
