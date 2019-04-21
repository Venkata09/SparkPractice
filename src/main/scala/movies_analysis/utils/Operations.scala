package movies_analysis.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, max, min}

object Operations {
  def minMaxAndAverageRatingForEachMovie(moviesDataFrame: DataFrame, ratingsDataFrame: DataFrame): DataFrame = {
    moviesDataFrame.join(
      ratingsDataFrame
        .groupBy("movieId")
        .agg(
          min("rating").as("ratingMin"),
          max("rating").as("ratingMax"),
          avg("rating").as("ratingAvg")),
      Seq("movieId"), // TODO: What is the Purpose of this SEQ(MovieID)
      "left")
  }

  def topThreeMoviesPerUser(ratingsDataFrame: DataFrame,
                            moviesDataFrame: DataFrame): DataFrame = {

    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions.{collect_list, row_number}
    import ratingsDataFrame.sqlContext.implicits._

    val window = Window.partitionBy("userId").orderBy('rating.desc, 'movieId.desc)

    ratingsDataFrame
      .withColumn("rank", row_number.over(window))
      .filter('rank <= 3)
      .join(moviesDataFrame, Seq("movieId"))  // TODO: What is the Purpose of this SEQ(MovieID)
      .groupBy("userId")
      .agg(collect_list("title").as("favouriteMovies"))
      .select("userId", "favouriteMovies")


  }

}
