package org.ekstep.analytics.adhoc

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig, Redis}
import org.ekstep.analytics.framework.FrameworkContext

import java.util.UUID

object CourseCommentsRatingsForNLW extends AbsDashboardModel {

  implicit val className: String = "org.ekstep.analytics.adhoc.CourseCommentsRatingsForNLW"

  override def name() = "CourseCommentsRatingsForNLW"

  /**
   * Master method, does all the work, fetching, processing and dispatching
   *
   * @param timestamp unique timestamp from the start of the processing
   */
  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    val today = getDate()
    //GET ORG DATA
    val (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()

    // Get course data first
    val allCourseProgramDetailsDF = contentWithOrgDetailsDataFrame(orgDF, Seq("Course"))

    val ratings = cache.load("rating")

    val timeUUIDToTimestampMills = udf((timeUUID: String) => (UUID.fromString(timeUUID).timestamp() - 0x01b21dd213814000L) / 10000)

    val ratingsDF = ratings.join(broadcast(allCourseProgramDetailsDF), ratings("activityid") === allCourseProgramDetailsDF("courseID"), "left")
      .withColumn("rated_on_tm", timeUUIDToTimestampMills(col("createdon")))
      //.withColumn("comment_updated_on", timeUUIDToTimestampMills(col("commentupdatedon")))
      .withColumn("rated_on_str",date_format(from_unixtime(col("rated_on_tm") / 1000), dateTimeFormat))
    //.withColumn("rated_on", date_format(col("rated_on_str"), dateTimeFormat))

    show(ratingsDF,"ratingsDF")

    val finalDF = ratingsDF.where(col("rated_on_str").geq(lit("2024-10-19 00:00:00")) && col("rated_on_str").lt(lit("2024-10-28 00:00:00")))
      .select(
        col("activityid").alias("course_id"),
        col("courseName"),
        col("comment"),
        col("recommended"),
        col("review"),
        col("rating"),
        col("rated_on_str")
      ).coalesce(1)

    show(finalDF, "ratingsDF")

    finalDF.coalesce(1).write.mode(SaveMode.Overwrite).format("csv").option("header", true).save(s"/tmp/${today}-NLW-course_ratings")

    Redis.closeRedisConnect()

  }
}

