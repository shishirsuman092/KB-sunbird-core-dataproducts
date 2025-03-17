package org.ekstep.analytics.dashboard.report.warehouse

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig}
import org.ekstep.analytics.framework.FrameworkContext


object DataWarehouseModel extends AbsDashboardModel {

  implicit val className: String = "org.ekstep.analytics.dashboard.report.warehouse.DataWarehouseModel"
  override def name() = "DataWarehouseModel"

  /**
   * Reading all the reports and saving it to postgres. Overwriting the data in postgres
   *
   * @param timestamp unique timestamp from the start of the processing
   */
  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {

    val dwPostgresUrl = s"jdbc:postgresql://${conf.dwPostgresHost}/${conf.dwPostgresSchema}"

    val userDetails = warehouseCache.load(conf.dwUserTable)
      .withColumn("status", col("status").cast("int"))
      .withColumn("no_of_karma_points", col("no_of_karma_points").cast("int"))
      .withColumn("marked_as_not_my_user", col("marked_as_not_my_user").cast("boolean"))
      .withColumn("total_event_learning_hours", col("total_event_learning_hours").cast("double"))
      .withColumn("total_content_learning_hours", col("total_content_learning_hours").cast("double"))
      .withColumn("total_learning_hours", col("total_learning_hours").cast("double"))
    truncateWarehouseTable(conf.dwUserTable)
    saveDataframeToPostgresTable_With_Append(userDetails, dwPostgresUrl, conf.dwUserTable, conf.dwPostgresUsername, conf.dwPostgresCredential)

    val contentDetails = warehouseCache.load(conf.dwCourseTable)
      .withColumn("resource_count", col("resource_count").cast("int"))
      .withColumn("total_certificates_issued", col("total_certificates_issued").cast("int"))
      .withColumn("content_rating", col("content_rating").cast("float"))
      .dropDuplicates(Seq("content_id"))
    truncateWarehouseTable(conf.dwCourseTable)
    saveDataframeToPostgresTable_With_Append(contentDetails, dwPostgresUrl, conf.dwCourseTable, conf.dwPostgresUsername, conf.dwPostgresCredential)

    val assessmentDetails =  warehouseCache.load(conf.dwAssessmentTable)
      .withColumn("score_achieved", col("score_achieved").cast("float"))
      .withColumn("overall_score", col("overall_score").cast("float"))
      .withColumn("cut_off_percentage", col("cut_off_percentage").cast("float"))
      .withColumn("total_question", col("total_question").cast("int"))
      .withColumn("number_of_incorrect_responses", col("number_of_incorrect_responses").cast("int"))
      .withColumn("number_of_retakes", col("number_of_retakes").cast("int"))
      .filter(col("content_id").isNotNull)
    truncateWarehouseTable(conf.dwAssessmentTable)
    saveDataframeToPostgresTable_With_Append(assessmentDetails, dwPostgresUrl, conf.dwAssessmentTable, conf.dwPostgresUsername, conf.dwPostgresCredential)

    val bpEnrollments =  warehouseCache.load(conf.dwBPEnrollmentsTable)
      .withColumn("component_progress_percentage", col("component_progress_percentage").cast("float"))
      .withColumn("offline_session_date", to_date(col("offline_session_date"), dateFormat))
      .withColumn("component_completed_on", to_date(col("component_completed_on"), dateFormat))
      .withColumn("last_accessed_on", to_date(col("last_accessed_on"), dateFormat))
      .withColumnRenamed("instructor(s)_name", "instructors_name")
      .filter(col("content_id").isNotNull)
      .filter(col("user_id").isNotNull)
      .filter(col("batch_id").isNotNull)
    truncateWarehouseTable(conf.dwBPEnrollmentsTable)
    saveDataframeToPostgresTable_With_Append(bpEnrollments, dwPostgresUrl, conf.dwBPEnrollmentsTable, conf.dwPostgresUsername, conf.dwPostgresCredential)

    val contentResourceDetails =  warehouseCache.load(conf.dwContentResourceTable)
    truncateWarehouseTable(conf.dwContentResourceTable)
    saveDataframeToPostgresTable_With_Append(contentResourceDetails, dwPostgresUrl, conf.dwContentResourceTable, conf.dwPostgresUsername, conf.dwPostgresCredential)

    val cbPlan =  warehouseCache.load(conf.dwCBPlanTable)
    truncateWarehouseTable(conf.dwCBPlanTable)
    saveDataframeToPostgresTable_With_Append(cbPlan, dwPostgresUrl, conf.dwCBPlanTable, conf.dwPostgresUsername, conf.dwPostgresCredential)

    val orgDwDf = cache.load("orgHierarchy")
      .withColumn("mdo_created_on", to_date(col("mdo_created_on")).cast("string")).cache()
    warehouseCache.write(orgDwDf.coalesce(1), conf.dwOrgTable)
    truncateWarehouseTable(conf.dwOrgTable)
    saveDataframeToPostgresTable_With_Append(orgDwDf, dwPostgresUrl, conf.dwOrgTable, conf.dwPostgresUsername, conf.dwPostgresCredential)

    val kcmContentCompetencyMapping =  warehouseCache.load(conf.dwKcmContentTable)
      .select(col("course_id"), col("competency_area_id"), col("competency_theme_id"), col("competency_sub_theme_id"), col("data_last_generated_on"))
    truncateWarehouseTable(conf.dwKcmContentTable)
    saveDataframeToPostgresTable_With_Append(kcmContentCompetencyMapping, dwPostgresUrl, conf.dwKcmContentTable, conf.dwPostgresUsername, conf.dwPostgresCredential)

    val kcmHierarchy =  warehouseCache.load(conf.dwKcmDictionaryTable)
    truncateWarehouseTable(conf.dwKcmDictionaryTable)
    saveDataframeToPostgresTable_With_Append(kcmHierarchy, dwPostgresUrl, conf.dwKcmDictionaryTable, conf.dwPostgresUsername, conf.dwPostgresCredential)

    val enrollmentDetails =  warehouseCache.load(conf.dwEnrollmentsTable)
      .withColumn("content_progress_percentage", col("content_progress_percentage").cast("float"))
      .withColumn("user_rating", col("user_rating").cast("float"))
      .withColumn("resource_count_consumed", col("resource_count_consumed").cast("int"))
      .withColumn("live_cbp_plan_mandate", col("live_cbp_plan_mandate").cast("boolean"))
      .filter(col("content_id").isNotNull)
    truncateWarehouseTable(conf.dwEnrollmentsTable)
    saveDataframeToPostgresTable_With_Append(enrollmentDetails, dwPostgresUrl, conf.dwEnrollmentsTable, conf.dwPostgresUsername, conf.dwPostgresCredential)

    val eventsDataDF = cache.load("eventDetails")
      .select(
        col("event_id"),col("event_name"),col("event_provider_mdo_id"),col("event_start_datetime"),
        col("duration"),col("event_status"),col("event_type"),col("presenters"),col("video_link"),col("recording_link"), col("event_tag")
      )
    truncateWarehouseTable(conf.dwEventsTable)
    saveDataframeToPostgresTable_With_Append(eventsDataDF, dwPostgresUrl, conf.dwEventsTable, conf.dwPostgresUsername, conf.dwPostgresCredential)
    warehouseCache.write(eventsDataDF, "event_details")

    val eventsEnrolmentDataDF = cache.load("eventEnrolmentDetails")
    val karmaPointsData = cache.load("userKarmaPoints")
      .select(col("userid").alias("user_id"),col("context_id").alias("event_id"),col("points"))
      .groupBy(col("user_id"), col("event_id")).agg(sum(col("points")).alias("karma_points"))
    val eventsEnrolmentDataDFWithKarmaPoints =  eventsEnrolmentDataDF.join(karmaPointsData, Seq("user_id", "event_id"), "left")
    truncateWarehouseTable("events_enrolment")
    saveDataframeToPostgresTable_With_Append(eventsEnrolmentDataDFWithKarmaPoints, dwPostgresUrl, "events_enrolment", conf.dwPostgresUsername, conf.dwPostgresCredential)
    warehouseCache.write(eventsEnrolmentDataDFWithKarmaPoints, "event_enrolment_details")
  }
}