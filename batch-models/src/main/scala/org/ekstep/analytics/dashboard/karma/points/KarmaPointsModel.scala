package org.ekstep.analytics.dashboard.karma.points

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig, Redis}
import org.ekstep.analytics.framework.FrameworkContext
import org.joda.time.DateTime

import java.util.UUID

object KarmaPointsModel extends AbsDashboardModel {
  implicit val className: String = "org.ekstep.analytics.dashboard.karma.points.KarmaPointsModel"

  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {

    val currentDate = DateTime.now()
    val monthStartMills = currentDate.minusMonths(1).withDayOfMonth(1).getMillis
    val monthEndMills = currentDate.withDayOfMonth(1).getMillis
    val monthStartSeconds = monthStartMills / 1000
    val monthEndSeconds = monthEndMills / 1000

    val timeUUIDToTimestampMills = udf((timeUUID: String) => (UUID.fromString(timeUUID).timestamp() - 0x01b21dd213814000L) / 10000)

    // filter only previous month's ratings
    val courseRatingKarmaPointsDF = userCourseRatingDataframe()
      .withColumn("credit_date", timeUUIDToTimestampMills(col("createdOn")))
      .where(s"credit_date >= '${monthStartMills}' AND credit_date < '${monthEndMills}'")
      .withColumn("credit_date", col("credit_date") / 1000)
    show(courseRatingKarmaPointsDF, "courseRatingKarmaPointsDF")

    // get cbp details like course name and category
    val categories = Seq("Course", "Program", "Blended Program", "CuratedCollections", "Standalone Assessment", "Curated Program")
    val cbpDetails = allCourseProgramESDataFrame(categories)
      .where("courseStatus IN ('Live', 'Retired')")
      .select("courseID", "courseName", "category")
    val courseDetails = cbpDetails.where("category='Course'")

    val karmaPointsFromRatingDF = courseRatingKarmaPointsDF
      .join(cbpDetails, Seq("courseID"), "left")
      .withColumn("operation_type", lit("RATING"))
      .withColumn("COURSENAME", col("courseName"))
      .withColumn("addinfo", to_json(struct("COURSENAME")))
      .withColumn("points", lit(2).cast("int"))
      .select(
        col("courseID").alias("context_id"),
        col("userID").alias("userid"),
        col("category").alias("context_type"),
        col("credit_date"),
        col("operation_type"),
        col("addinfo"),
        col("points")
      )
    show(karmaPointsFromRatingDF, "karmaPointsFromRatingDF")

    val userCourseCompletionDF = userCourseProgramCompletionDataFrame(datesAsLong = true)
      .where(s"dbCompletionStatus=2 AND courseCompletedTimestamp >= '${monthStartSeconds}' AND courseCompletedTimestamp < '${monthEndSeconds}'")
      .join(courseDetails, Seq("courseID"), "inner")
    show(userCourseCompletionDF, "userCourseCompletionDF")

    val firstCompletionDataDF = userCourseCompletionDF
      .groupByLimit(Seq("userID"), "courseCompletedTimestamp", 4)
      .drop("rowNum")
    show(firstCompletionDataDF, "firstCompletionDataDF")

    val coursesWithAssessmentDF = cache.load("userAssessment")
      .where("assessUserStatus='SUBMITTED' AND assessChildID IS NOT NULL")
      .select("courseID")
      .distinct()
      .withColumn("hasAssessment", lit(true))
    show(coursesWithAssessmentDF, "coursesWithAssessmentDF")

    val karmaPointsFromCourseCompletionDF = firstCompletionDataDF
      .join(coursesWithAssessmentDF, Seq("courseID"), "left")
      .na.fill(value = false, Seq("hasAssessment"))
      .withColumn("operation_type", lit("COURSE_COMPLETION"))
      .withColumn("COURSE_COMPLETION", lit(true))
      .withColumn("COURSENAME", col("courseName"))
      .withColumn("ACBP", lit(false))
      .withColumn("ASSESSMENT", col("hasAssessment"))
      .withColumn("addinfo", to_json(struct("COURSE_COMPLETION", "COURSENAME", "ACBP", "ASSESSMENT")))
      .withColumn("points", when(col("hasAssessment"), lit(10)).otherwise(lit(5)))
      .select(
        col("courseID").alias("context_id"),
        col("userID").alias("userid"),
        col("category").alias("context_type"),
        col("courseCompletedTimestamp").alias("credit_date"),
        col("operation_type"),
        col("addinfo"),
        col("points")
      )
    show(karmaPointsFromCourseCompletionDF, "karmaPointsFromCourseCompletionDF")

    // union both karma points dataframes, and write to cassandra
    val allKarmaPointsDF = karmaPointsFromRatingDF.union(karmaPointsFromCourseCompletionDF)
      .withColumn("credit_date", col("credit_date").cast(TimestampType))
    show(allKarmaPointsDF, "allKarmaPointsDF")
    writeToCassandra(allKarmaPointsDF, conf.cassandraUserKeyspace, conf.cassandraKarmaPointsTable)

    // write to lookup table
    val lookupDataDF = allKarmaPointsDF
      .select("userid", "context_type", "context_id", "operation_type", "credit_date")
      .withColumn("user_karma_points_key", concat_ws("|", col("userid"), col("context_type"), col("context_id")))
      .drop("userid", "context_type", "context_id")
    show(lookupDataDF, "lookupDataDF")
    writeToCassandra(lookupDataDF, conf.cassandraUserKeyspace, conf.cassandraKarmaPointsLookupTable)

    // update summary table
    val existingSummaryData = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraKarmaPointsSummaryTable)
      .select(col("userid"), col("total_points").alias("existing_total_points"))
    show(existingSummaryData, "existingSummaryData")

    val summaryDataDF = allKarmaPointsDF
      .select("userid", "points", "context_id")
      .groupBy("userid")
      .agg(sum(col("points")).alias("points"))
      .join(existingSummaryData, Seq("userid"), "full")
      .na.fill(0, Seq("existing_total_points", "points"))
      .withColumn("total_points", expr("existing_total_points + points"))
      .select("userid", "total_points")
    show(summaryDataDF, "summaryDataDF")
    writeToCassandra(summaryDataDF, conf.cassandraUserKeyspace, conf.cassandraKarmaPointsSummaryTable)

    Redis.closeRedisConnect()
  }

}
