package activityRunnerProject

object ActivityRunner extends App with Helper {
  val activityFile = args(0)
  val visitorsFile = args(1)
  val usersFile = args(2)
  val outputDir = args(3)

  final val activityHeaders = Seq("visitorId", "email", "timestamp")
  final val visitorsHeaders = Seq("visitorId", "userId")
  final val usersHeaders = Seq("userId", "email")

  /**
    * Create dataframes from raw files and headers.
    */
  val activityDf = createDf(activityFile, activityHeaders)
  val visitorsDf = createDf(visitorsFile, visitorsHeaders)
  val usersDf = createDf(usersFile, usersHeaders)

  /**
    * Find activity based on email.
    */
  val emailActivityDf =
    activityDf
      .join(usersDf, "email")
      .select("userId", "timestamp")

  /**
    * Find activity based on visitorId.
    */
  val visitorActivityDf =
    activityDf
      .filter("email is null or email = ''")
      .join(visitorsDf, "visitorId")
      .select("userId", "timestamp")

  /**
    * Aggregate the activity.
    */
  val userActivityDf = emailActivityDf.unionAll(visitorActivityDf)

  /**
    * Output the file.
    */
  writeDfToOutput(userActivityDf, outputDir)
}
