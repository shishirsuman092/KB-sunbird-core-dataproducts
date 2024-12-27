package org.ekstep.analytics.dashboard.helpers.verify

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.FloatType
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig, Redis}
import org.ekstep.analytics.framework._


object ValidateDashboardModel extends AbsDashboardModel {

  implicit val className: String = "org.ekstep.analytics.dashboard.helpers.verify.ValidateDashboardModel"
  override def name() = "ValidateDashboardModel"

  /**
   * Master method, does all the work, fetching, processing and dispatching
   *
   * @param timestamp unique timestamp from the start of the processing
   */
  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {

    // get user org data frames, no filtering is done here
    var (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()
    val roleDF = roleDataFrame()
    val userOrgRoleDF = userOrgRoleDataFrame(userOrgDF, roleDF)

    // get all rows from user_enrolments table, only filtering out active=false
    val userCourseProgramCompletionDF = userCourseProgramCompletionDataFrame(datesAsLong = true)

    // get the content hierarchy table
    val hierarchyDF = contentHierarchyDataFrame()

    // get only the ids in enrolments table to filter the hierarchy table
    val progressCourseIDs = userCourseProgramCompletionDF.select(col("courseID")).distinct()

    // get filtered hierarchy
    val filteredHierarchyDF = addHierarchyColumn(progressCourseIDs, hierarchyDF, "courseID", "data")

    // prepare org df for clean joining
    val courseJoinOrgDF = orgDF.select(
      col("orgID").alias("courseOrgID"),
      col("orgName").alias("courseOrgName"),
      col("orgStatus").alias("courseOrgStatus")
    )

    val userCourseProgramCompletionWithDetailsDF = userCourseProgramCompletionDF
      .join(filteredHierarchyDF, Seq("courseID"), "left")
      .withColumn("courseOrgID", col("data.channel"))
      .withColumn("courseName", col("data.name"))
      .withColumn("courseStatus", col("data.status"))
      .withColumn("courseDuration", col("data.duration").cast(FloatType))
      .withColumn("category", col("data.primaryCategory"))
      .withColumn("courseReviewStatus", col("data.reviewStatus"))
      .join(userOrgDF, Seq("userID"), "left")
      .join(courseJoinOrgDF, Seq("courseOrgID"), "left")

    // filter
    val filteredProgressDF = userCourseProgramCompletionWithDetailsDF
      .where(expr("category='Course' AND courseStatus IN ('Live', 'Retired') AND userStatus=1"))

    println(s"Before join: ${userCourseProgramCompletionDF.count()}")
    println(s"Before filter: ${userCourseProgramCompletionWithDetailsDF.count()}")
    println(s"After filter: ${filteredProgressDF.count()}")

    // course df with counts
    val courseProgressCountsDF = filteredProgressDF
      .groupBy("courseID", "courseName", "courseOrgID", "courseOrgName")
      .agg(
        expr("COUNT(userID)").alias("enrolled_count"),
        expr("SUM(CASE WHEN dbCompletionStatus=0 THEN 1 ELSE 0 END)").alias("not_started_count"),
        expr("SUM(CASE WHEN dbCompletionStatus=1 THEN 1 ELSE 0 END)").alias("in_progress_count"),
        expr("SUM(CASE WHEN dbCompletionStatus=2 THEN 1 ELSE 0 END)").alias("completed_count")
      )

    val mdoOnboardedDF = userOrgRoleDF
      .where(expr("userStatus=1 AND userOrgStatus=1 AND role='MDO_ADMIN'"))
      .agg(expr("COUNT(DISTINCT userOrgID)").as("mdoOnboarded"))


    val top5CoursesByCompletionDF = courseProgressCountsDF
      .orderBy(col("completed_count").desc).limit(5)

    val top5MDOsByCompletionDF = filteredProgressDF
      .where(expr("userOrgStatus=1 AND dbCompletionStatus=2"))
      .groupBy("userOrgID", "userOrgName")
      .agg(expr("COUNT(userID)").alias("completed_count"))
      .orderBy(col("completed_count").desc).limit(5)

    val enrolmentByStatusDF = filteredProgressDF
      .agg(
        expr("COUNT(userID)").alias("enrolled_count"),
        expr("SUM(CASE WHEN dbCompletionStatus=0 THEN 1 ELSE 0 END)").alias("not_started_count"),
        expr("SUM(CASE WHEN dbCompletionStatus=1 THEN 1 ELSE 0 END)").alias("in_progress_count"),
        expr("SUM(CASE WHEN dbCompletionStatus=2 THEN 1 ELSE 0 END)").alias("completed_count")
      )

    val uniqueUsersEnrolledDF = filteredProgressDF
      .agg(
        expr("COUNT(DISTINCT(userID))").alias("unique_users_enrolled")
      )

    val uniqueUsersCompletedDF = filteredProgressDF
      .where(expr("dbCompletionStatus=2"))
      .agg(
        expr("COUNT(DISTINCT(userID))").alias("unique_users_completed")
      )

    Redis.closeRedisConnect()

  }

}