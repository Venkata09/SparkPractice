package movies_analysis

import movies_analysis.utils.{Operations, Reader, Writer}

object Job {
  def run(inputMoviesPath: String, inputRatingsPath: String) {
    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")
    val movies = Reader.readMovies(inputMoviesPath)
    val ratings = Reader.readRatings(inputRatingsPath)

    val movieRatings = Operations.minMaxAndAverageRatingForEachMovie(movies, ratings)
    val topThree = Operations.topThreeMoviesPerUser(ratings, movies)

    movieRatings.show()

    topThree.show()



    /* Writer.write(movies, s"$OutputPath/movies")
     Writer.write(ratings, s"$OutputPath/ratings")
     Writer.write(movieRatings, s"$OutputPath/movieRatings")
     Writer.write(topThree, s"$OutputPath/topThree")*/

  }
}
