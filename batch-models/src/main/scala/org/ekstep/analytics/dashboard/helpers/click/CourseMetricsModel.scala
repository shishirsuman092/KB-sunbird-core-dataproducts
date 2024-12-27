package org.ekstep.analytics.dashboard.helpers.click

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig, Redis}
import org.ekstep.analytics.framework._


object CourseMetricsModel extends AbsDashboardModel {

  implicit val className: String = "org.ekstep.analytics.dashboard.helpers.click.CourseMetricsModel"
  override def name() = "CourseMetricsModel"

  /**
   * Master method, does all the work, fetching, processing and dispatching
   *
   * @param timestamp unique timestamp from the start of the processing
   */
  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {

    // obtain and save user org data
    var (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()

    // get course details, attach rating info, dispatch to kafka to be ingested by druid data-source: dashboards-courses
    val (hierarchyDF, allCourseProgramDetailsWithCompDF, allCourseProgramDetailsDF,
      allCourseProgramDetailsWithRatingDF) = contentDataFrames(orgDF, Seq("Course", "Program", "CuratedCollections","Curated Program"))

    val readLoc = "/home/analytics/click-stream-data/"
    val loc = "/home/analytics/click-stream-data/gen/"

    // object_id, object_type, clicks
    val clickDF = spark.read.format("csv").option("header", "true")
      .load(s"${readLoc}clicks-by-content-id.csv")
      .select(
        col("object_id").alias("courseID"),
        col("object_type").alias("category"),
        col("clicks")
      )
    val clickWithDetailsDF = clickDF.join(allCourseProgramDetailsWithRatingDF, Seq("courseID", "category"), "left")

    // click by id
    val clickByIDDF = clickWithDetailsDF.select("courseID", "courseName", "category", "clicks")
      .orderBy(desc("clicks"))
    csvWrite(clickByIDDF.coalesce(1), s"${loc}clicks-by-content-name.csv")

    // click by provider
    val clickByProviderDF = clickWithDetailsDF.groupBy("courseOrgID", "courseOrgName")
      .agg(expr("SUM(clicks)").alias("clicks"))
      .orderBy(desc("clicks"))
    csvWrite(clickByProviderDF.coalesce(1), s"${loc}clicks-by-provider.csv")

    // click by course duration
    val clickByDuration = clickWithDetailsDF.withColumn("durationFloorHrs", expr("CAST(FLOOR(courseDuration / 3600) AS INTEGER)"))
      .groupBy("durationFloorHrs")
      .agg(expr("SUM(clicks)").alias("clicks"))
      .orderBy("durationFloorHrs")
    csvWrite(clickByDuration.coalesce(1), s"${loc}clicks-by-duration.csv")

    // click by course rating
    val clickByRating = clickWithDetailsDF.withColumn("ratingFloor", expr("CAST(FLOOR(ratingAverage) AS INTEGER)"))
      .groupBy("ratingFloor")
      .agg(expr("SUM(clicks)").alias("clicks"))
      .orderBy("ratingFloor")
    csvWrite(clickByRating.coalesce(1), s"${loc}clicks-by-rating.csv")

    val allCourseProgramCompetencyDF = allCourseProgramCompetencyDataFrame(allCourseProgramDetailsWithCompDF)
    val clickWithCompetencyDF = clickDF.join(allCourseProgramCompetencyDF, Seq("courseID", "category"), "left")

    // click by competencies
    val clickByCompetencyDF = clickWithCompetencyDF.groupBy("competencyID", "competencyName")
      .agg(expr("SUM(clicks)").alias("clicks"))
      .orderBy(desc("clicks"))
    csvWrite(clickByCompetencyDF.coalesce(1), s"${loc}clicks-by-comp.csv")

    // get course completion data, dispatch to kafka to be ingested by druid data-source: dashboards-user-course-program-progress
    val userCourseProgramCompletionDF = userCourseProgramCompletionDataFrame(datesAsLong = true)
    val allCourseProgramCompletionWithDetailsDF = allCourseProgramCompletionWithDetailsDataFrame(userCourseProgramCompletionDF, allCourseProgramDetailsDF, userOrgDF)
    validate({userCourseProgramCompletionDF.count()}, {allCourseProgramCompletionWithDetailsDF.count()}, "userCourseProgramCompletionDF.count() should equal final course progress DF count")

    val contentUserStatusCountDF = allCourseProgramCompletionWithDetailsDF
      .where(expr("courseStatus IN ('Live', 'Retired')"))
      .groupBy("courseID", "courseName", "category")
      .agg(
        expr("COUNT(courseID)").alias("countEnrolled"),
        expr("SUM(CASE WHEN dbCompletionStatus=1 THEN 1 ELSE 0 END)").alias("countInProgress"),
        expr("SUM(CASE WHEN dbCompletionStatus=2 THEN 1 ELSE 0 END)").alias("countCompleted")
      )
    val clickWithProgressCountsDF = clickDF.join(contentUserStatusCountDF, Seq("courseID", "category"), "left")

    val clickByEnroll = bucketGroupBy(clickWithProgressCountsDF, "countEnrolled", "clicks")
    csvWrite(clickByEnroll.coalesce(1),s"${loc}clicks-by-enroll.csv")

    val clickByInProgress = bucketGroupBy(clickWithProgressCountsDF, "countInProgress", "clicks")
    csvWrite(clickByInProgress.coalesce(1), s"${loc}clicks-by-in-progress.csv")

    val clickByCompleted = bucketGroupBy(clickWithProgressCountsDF, "countCompleted", "clicks")
    csvWrite(clickByCompleted.coalesce(1),s"${loc}clicks-by-completed.csv")

    Redis.closeRedisConnect()
  }

  def bucketGroupBy(df: DataFrame, bucketCol: String, groupCol: String): DataFrame = {
    val maxVal = df.agg(expr(s"MAX(${bucketCol})")).head().getLong(0)  // 12345
    val orderMaxVal = Math.log10(maxVal).floor.toLong  // 4
    val order = Math.pow(10, orderMaxVal - 1).toLong  // 1000

    df.withColumn(s"${bucketCol}Floor", expr(s"CAST((FLOOR(${bucketCol} / ${order}) * ${order}) AS LONG)"))
      .groupBy(s"${bucketCol}Floor")
      .agg(expr(s"SUM(${groupCol})").alias(s"${groupCol}"))
      .orderBy(s"${bucketCol}Floor")
  }

}