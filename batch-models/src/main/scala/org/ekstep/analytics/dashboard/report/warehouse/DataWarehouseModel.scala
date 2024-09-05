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

    val today = getDate()
    val dwPostgresUrl = s"jdbc:postgresql://${conf.dwPostgresHost}/${conf.dwPostgresSchema}"

    def readCSV(path: String): DataFrame = {
      spark.read.option("header", "true").csv(path)
    }

    val userDetailsPath = s"${conf.localReportDir}/${conf.userReportPath}/${today}-warehouse"
    val userDetails = readCSV(userDetailsPath)
      .withColumn("status", col("status").cast("int"))
      .withColumn("no_of_karma_points", col("no_of_karma_points").cast("int"))
      .withColumn("marked_as_not_my_user", col("marked_as_not_my_user").cast("boolean"))
    truncateWarehouseTable(conf.dwUserTable)
    saveDataframeToPostgresTable_With_Append(userDetails, dwPostgresUrl, conf.dwUserTable, conf.dwPostgresUsername, conf.dwPostgresCredential)

    val contentDetailsPath = s"${conf.localReportDir}/${conf.courseReportPath}/${today}-warehouse"
    val contentDetails = readCSV(contentDetailsPath)
      .withColumn("resource_count", col("resource_count").cast("int"))
      .withColumn("total_certificates_issued", col("total_certificates_issued").cast("int"))
      .withColumn("content_rating", col("content_rating").cast("float"))
      .dropDuplicates(Seq("content_id"))
    truncateWarehouseTable(conf.dwCourseTable)
    saveDataframeToPostgresTable_With_Append(contentDetails, dwPostgresUrl, conf.dwCourseTable, conf.dwPostgresUsername, conf.dwPostgresCredential)

    val assessmentDetailsPath = s"${conf.localReportDir}/${conf.cbaReportPath}/${today}-warehouse"
    val assessmentDetails = readCSV(assessmentDetailsPath)
      .withColumn("score_achieved", col("score_achieved").cast("float"))
      .withColumn("overall_score", col("overall_score").cast("float"))
      .withColumn("cut_off_percentage", col("cut_off_percentage").cast("float"))
      .withColumn("total_question", col("total_question").cast("int"))
      .withColumn("number_of_incorrect_responses", col("number_of_incorrect_responses").cast("int"))
      .withColumn("number_of_retakes", col("number_of_retakes").cast("int"))
      .filter(col("content_id").isNotNull)
    truncateWarehouseTable(conf.dwAssessmentTable)
    saveDataframeToPostgresTable_With_Append(assessmentDetails, dwPostgresUrl, conf.dwAssessmentTable, conf.dwPostgresUsername, conf.dwPostgresCredential)

    val bpEnrollmentsPath = s"${conf.localReportDir}/${conf.blendedReportPath}/${today}-warehouse"
    val bpEnrollments = readCSV(bpEnrollmentsPath)
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

    val contentResourcePath = s"${conf.localReportDir}/${conf.courseReportPath}/${today}-resource-warehouse"
    val contentResourceDetails = readCSV(contentResourcePath)
    truncateWarehouseTable(conf.dwContentResourceTable)
    saveDataframeToPostgresTable_With_Append(contentResourceDetails, dwPostgresUrl, conf.dwContentResourceTable, conf.dwPostgresUsername, conf.dwPostgresCredential)

    val cbPlanPath = s"${conf.localReportDir}/${conf.acbpReportPath}/${today}-warehouse"
    val cbPlan = readCSV(cbPlanPath)
    truncateWarehouseTable(conf.dwCBPlanTable)
    saveDataframeToPostgresTable_With_Append(cbPlan, dwPostgresUrl, conf.dwCBPlanTable, conf.dwPostgresUsername, conf.dwPostgresCredential)

    val orgDwDf = cache.load("orgHierarchy")
      .withColumn("mdo_created_on", to_date(col("mdo_created_on"))).cache()
    generateReport(orgDwDf.coalesce(1), s"${conf.orgHierarchyReportPath}/${today}-warehouse")
    truncateWarehouseTable(conf.dwOrgTable)
    saveDataframeToPostgresTable_With_Append(orgDwDf, dwPostgresUrl, conf.dwOrgTable, conf.dwPostgresUsername, conf.dwPostgresCredential)

    val kcmContentCompetencyMappingPath = s"${conf.localReportDir}/${conf.kcmReportPath}/${today}/ContentCompetencyMapping-warehouse"
    val kcmContentCompetencyMapping = readCSV(kcmContentCompetencyMappingPath)
      .select(col("course_id"), col("competency_area_id").cast("int"), col("competency_theme_id").cast("int"), col("competency_sub_theme_id").cast("int"))
    truncateWarehouseTable(conf.dwKcmContentTable)
    saveDataframeToPostgresTable_With_Append(kcmContentCompetencyMapping, dwPostgresUrl, conf.dwKcmContentTable, conf.dwPostgresUsername, conf.dwPostgresCredential)

    val kcmHierarchyPath = s"${conf.localReportDir}/${conf.kcmReportPath}/${today}/CompetencyHierarchy-warehouse"
    val kcmHierarchy = readCSV(kcmHierarchyPath)
      .withColumn("competency_area_id", col("competency_area_id").cast("int"))
      .withColumn("competency_theme_id", col("competency_theme_id").cast("int"))
      .withColumn("competency_sub_theme_id", col("competency_sub_theme_id").cast("int"))
    truncateWarehouseTable(conf.dwKcmDictionaryTable)
    saveDataframeToPostgresTable_With_Append(kcmHierarchy, dwPostgresUrl, conf.dwKcmDictionaryTable, conf.dwPostgresUsername, conf.dwPostgresCredential)

    val enrollmentDetailsPath = s"${conf.localReportDir}/${conf.userEnrolmentReportPath}/${today}-warehouse"
    val enrollmentDetails = readCSV(enrollmentDetailsPath)
      .withColumn("content_progress_percentage", col("content_progress_percentage").cast("float"))
      .withColumn("user_rating", col("user_rating").cast("float"))
      .withColumn("resource_count_consumed", col("resource_count_consumed").cast("int"))
      .withColumn("live_cbp_plan_mandate", col("live_cbp_plan_mandate").cast("boolean"))
      .filter(col("content_id").isNotNull)
    truncateWarehouseTable(conf.dwEnrollmentsTable)
    saveDataframeToPostgresTable_With_Append(enrollmentDetails, dwPostgresUrl, conf.dwEnrollmentsTable, conf.dwPostgresUsername, conf.dwPostgresCredential)
  }
}
