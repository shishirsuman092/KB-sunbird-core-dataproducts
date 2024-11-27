package org.ekstep.analytics.dashboard.activity.user

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig, Redis}
import org.ekstep.analytics.framework.FrameworkContext

object UserActivityModel extends AbsDashboardModel {

  implicit val className: String = "org.ekstep.analytics.dashboard.activity.user.UserActivityModel"

  override def name() = "UserActivityModel"

  /**
   * Master method, does all the work, fetching, processing and dispatching
   *
   * @param timestamp unique timestamp from the start of the processing
   */
  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    val today = getDate()
    //GET ORG DATA
    val (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()
    val orgHierarchyData = orgHierarchyDataframe()
    val userDataDF = userOrgDF
      .join(broadcast(orgHierarchyData), Seq("userOrgID"), "left")
      .withColumn("designation", coalesce(col("professionalDetails.designation"), lit("")))

    // load avro files
    // course avro
    val contentDF = warehouseCache.load(conf.dwCourseTable)
      .withColumnRenamed("content_id", "id")
      .withColumnRenamed("batch_id", "c_batch_id")
      .drop("data_last_generated_on")
    // enrolment avro
    val warehouseDF = warehouseCache.load(conf.dwEnrollmentsTable)
    // event details avro
    val eventDetails = cache.load("eventDetails").withColumnRenamed("event_id", "ed_event_id")
    // event enrolment avro
    val eventEnrolments = cache.load("eventEnrolmentDetails")

    // processing event data

    // joining event enrolment table with event details table to get event type
    val eventEnrolmentWithDetails = eventEnrolments.join(broadcast(eventDetails), eventEnrolments("event_id") === eventDetails("ed_event_id"), "left")
    // creating final data for event enrolment
    val eventEnrolmentsDF = eventEnrolmentWithDetails.join(broadcast(userDataDF), eventEnrolments("user_id") === userDataDF("userID"), "left")
      .withColumn("certificate_generated", when(col("certificate_id").isNotNull or col("certificate_id") =!= "", lit(true)).otherwise(lit(false)))
      .withColumn("batch_id", lit("NA"))
      .withColumn("enrolled_on", col("enrolled_on_datetime").cast("timestamp"))
      .withColumn("resource_count_consumed", lit(1))
      .withColumn("user_rating", lit(0))
      .withColumn("live_cbp_plan_mandate", lit(false))
      .withColumn("data_last_generated_on", currentDateTime)
      .withColumn("number_of_certificate", when(col("certificate_id").isNotNull or col("certificate_id") =!= "", lit(1)).otherwise(lit(0)))
      .select(
        col("user_id"),
        col("batch_id"),
        col("event_id").alias("content_id"),
        col("event_type").alias("content_type"),
        col("enrolled_on"),
        col("completion_percentage").alias("content_progress_percentage"),
        col("resource_count_consumed"),
        col("status").alias("user_consumption_status"),
        col("completed_on_datetime").alias("first_completed_on"),
        col("completed_on_datetime").alias("first_certificate_generated_on"),
        col("completed_on_datetime").alias("last_completed_on"),
        col("completed_on_datetime").alias("last_certificate_generated_on"),
        col("completed_on_datetime").alias("content_last_accessed_on"),
        col("certificate_generated"),
        col("number_of_certificate"),
        col("user_rating"),
        col("certificate_id"),
        col("live_cbp_plan_mandate"),
        col("data_last_generated_on")
      ).dropDuplicates("user_id", "content_id", "batch_id")

    // processing content data
    val courseEnrolmentWithDetails = warehouseDF.join(broadcast(contentDF), warehouseDF("content_id") === contentDF("id"), "left")

    val contentEnrolmentWithDetails = courseEnrolmentWithDetails
      .withColumn("user_rating_cast", col("user_rating").cast("int"))
      .withColumn("certificate_generated_cast", col("certificate_generated").cast("boolean"))
      .select(
        col("user_id"),
        col("batch_id"),
        col("content_id"),
        col("content_type"),
        col("enrolled_on"),
        col("content_progress_percentage"),
        col("resource_count_consumed"),
        col("user_consumption_status"),
        col("first_completed_on"),
        col("first_certificate_generated_on"),
        col("last_completed_on"),
        col("last_certificate_generated_on"),
        col("content_last_accessed_on"),
        col("certificate_generated_cast").alias("certificate_generated"),
        col("number_of_certificate"),
        col("user_rating_cast").alias("user_rating"),
        col("certificate_id"),
        col("live_cbp_plan_mandate"),
        col("data_last_generated_on")
      ).dropDuplicates("user_id", "batch_id", "content_id")

    val userActivityDF = contentEnrolmentWithDetails.union(eventEnrolmentsDF)

    show(userActivityDF, "userActivityDF")
    val dwPostgresUrl = s"jdbc:postgresql://${conf.dwPostgresHost}/${conf.dwPostgresSchema}"
    truncateWarehouseTable(conf.dwUserActivityTable)
    saveDataframeToPostgresTable_With_Append(userActivityDF, dwPostgresUrl, conf.dwUserActivityTable, conf.dwPostgresUsername, conf.dwPostgresCredential)

    Redis.closeRedisConnect()
  }
}