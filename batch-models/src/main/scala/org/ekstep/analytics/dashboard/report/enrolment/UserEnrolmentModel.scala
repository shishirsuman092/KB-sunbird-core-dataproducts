package org.ekstep.analytics.dashboard.report.enrolment

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig, Redis}
import org.ekstep.analytics.framework.FrameworkContext


object UserEnrolmentModel extends AbsDashboardModel {

  implicit val className: String = "org.ekstep.analytics.dashboard.report.enrolment.UserEnrolmentModel"

  override def name() = "UserEnrolmentModel"

  /**
   * Master method, does all the work, fetching, processing and dispatching
   *
   * @param timestamp unique timestamp from the start of the processing
   */
  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    val today = getDate()

    //GET ORG DATA
    var (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()
    val orgHierarchyData = orgHierarchyDataframe()
    val userDataDF = userOrgDF
      .join(orgHierarchyData, Seq("userOrgID"), "left")
      .withColumn("designation", coalesce(col("professionalDetails.designation"), lit("")))
    show(userDataDF, "userDataDF")

    // Get course data first
    val allCourseProgramDetailsDF = contentWithOrgDetailsDataFrame(orgDF, Seq("Course", "Program", "Blended Program", "CuratedCollections", "Curated Program"))

    val userEnrolmentDF = userCourseProgramCompletionDataFrame()
    show(userEnrolmentDF, "userEnrolmentDF")

    val userRatingDF = userCourseRatingDataframe()

    //use allCourseProgramDetailsDFWithOrgName below instead of allCourseProgramDetailsDF after adding orgname alias above
    val allCourseProgramCompletionWithDetailsDF = allCourseProgramCompletionWithDetailsDataFrame(userEnrolmentDF, allCourseProgramDetailsDF, userDataDF)
    show(allCourseProgramCompletionWithDetailsDF, "allCourseProgramCompletionWithDetailsDF")

    val courseBatchDF = courseBatchDataFrame()
    val relevantBatchInfoDF = allCourseProgramDetailsDF.select("courseID", "category")
      .where(expr("category IN ('Blended Program')"))
      .join(courseBatchDF, Seq("courseID"), "left")
      .select("courseID", "batchID", "courseBatchName", "courseBatchStartDate", "courseBatchEndDate")
    show(relevantBatchInfoDF, "relevantBatchInfoDF")

    val allCourseProgramCompletionWithDetailsWithBatchInfoDF = allCourseProgramCompletionWithDetailsDF.join(relevantBatchInfoDF, Seq("courseID", "batchID"), "left")
    show(allCourseProgramCompletionWithDetailsWithBatchInfoDF, "allCourseProgramCompletionWithDetailsWithBatchInfoDF")

    val allCourseProgramCompletionWithDetailsDFWithRating = allCourseProgramCompletionWithDetailsWithBatchInfoDF.join(userRatingDF, Seq("courseID", "userID"), "left")

    var df = allCourseProgramCompletionWithDetailsDFWithRating
      .durationFormat("courseDuration")
      .withColumn("completedOn", date_format(col("courseCompletedTimestamp"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("enrolledOn", date_format(col("courseEnrolledTimestamp"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("firstCompletedOn", date_format(col("firstCompletedOn"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("lastContentAccessTimestamp", date_format(col("lastContentAccessTimestamp"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("courseLastPublishedOn", to_date(col("courseLastPublishedOn"), "dd/MM/yyyy"))
      .withColumn("courseBatchStartDate", to_date(col("courseBatchStartDate"), "dd/MM/yyyy"))
      .withColumn("courseBatchEndDate", to_date(col("courseBatchEndDate"), "dd/MM/yyyy"))
      .withColumn("completionPercentage", round(col("completionPercentage"), 2))
      .withColumn("Tag", concat_ws(", ", col("additionalProperties.tag")))
      .withColumn("Report_Last_Generated_On", date_format(current_timestamp(), "dd/MM/yyyy HH:mm:ss a"))
      .withColumn("Certificate_Generated", expr("CASE WHEN issuedCertificateCount > 0 THEN 'Yes' ELSE 'No' END"))
      .withColumn("ArchivedOn", expr("CASE WHEN courseStatus == 'Retired' THEN lastStatusChangedOn ELSE '' END"))
      .withColumn("ArchivedOn", to_date(col("ArchivedOn"), "dd/MM/yyyy"))
      .withColumn("Certificate_ID", col("certificateID"))

    df = df.distinct().dropDuplicates("userID", "courseID", "batchID")

    // read acbp data and filter the cbp plan based on status
    val acbpDF = acbpDetailsDF().where(col("acbpStatus") === "Live")

    val selectColumns = Seq("userID", "designation", "userOrgID", "acbpID", "assignmentType", "acbpCourseIDList","acbpStatus")
    val acbpAllotmentDF = explodedACBPDetails(acbpDF, userDataDF, selectColumns)

    // replace content list with names of the courses instead of ids
    var acbpAllEnrolmentDF = acbpAllotmentDF
      .withColumn("courseID", explode(col("acbpCourseIDList"))).withColumn("liveCBPlan", lit(true))

    acbpAllEnrolmentDF = acbpAllEnrolmentDF.select(col("userOrgID"),col("courseID"),col("userID"),col("designation"),col("liveCBPlan"))
    show(acbpAllEnrolmentDF, "acbpAllEnrolmentDF")

    df = df.join(acbpAllEnrolmentDF, Seq("userID", "userOrgID", "courseID"), "left")
      .withColumn("live_cbp_plan_mandate", when(col("liveCBPlan").isNull, false).otherwise(col("liveCBPlan")))

    val fullReportDF = df.select(
        col("userID"),
        col("userOrgID"),
        col("courseID"),
        col("courseOrgID"),
        col("fullName").alias("Full_Name"),
        col("professionalDetails.designation").alias("Designation"),
        col("personalDetails.primaryEmail").alias("Email"),
        col("personalDetails.mobile").alias("Phone_Number"),
        col("professionalDetails.group").alias("Group"),
        col("Tag"),
        col("ministry_name").alias("Ministry"),
        col("dept_name").alias("Department"),
        col("userOrgName").alias("Organization"),
        col("courseOrgName").alias("Content_Provider"),
        col("courseName").alias("Content_Name"),
        col("category").alias("Content_Type"),
        col("courseDuration").alias("Content_Duration"),
        col("batchID").alias("Batch_Id"),
        col("courseBatchName").alias("Batch_Name"),
        col("courseBatchStartDate").alias("Batch_Start_Date"),
        col("courseBatchEndDate").alias("Batch_End_Date"),
        col("enrolledOn").alias("Enrolled_On"),
        col("userCourseCompletionStatus").alias("Status"),
        col("completionPercentage").alias("Content_Progress_Percentage"),
        col("courseLastPublishedOn").alias("Last_Published_On"),
        col("ArchivedOn").alias("Content_Retired_On"),
        col("completedOn").alias("Completed_On"),
        col("Certificate_Generated"),
        col("userRating").alias("User_Rating"),
        col("personalDetails.gender").alias("Gender"),
        col("personalDetails.category").alias("Category"),
        col("additionalProperties.externalSystem").alias("External_System"),
        col("additionalProperties.externalSystemId").alias("External_System_Id"),
        col("userOrgID").alias("mdoid"),
        col("issuedCertificateCount"),
        col("courseStatus"),
        col("courseResourceCount").alias("resourceCount"),
        col("courseProgress").alias("resourcesConsumed"),
        round(expr("CASE WHEN courseResourceCount=0 THEN 0.0 ELSE 100.0 * courseProgress / courseResourceCount END"), 2).alias("rawCompletionPercentage"),
        col("Certificate_ID"),
        col("Report_Last_Generated_On"),
        col("live_cbp_plan_mandate").alias("Live_CBP_Plan_Mandate")
      )
      .coalesce(1)

    show(df, "df")

    val reportPath = s"${conf.userEnrolmentReportPath}/${today}"
    // generateReport(fullReportDF, s"${reportPath}-full")

    val mdoReportDF = fullReportDF.drop("userID", "userOrgID", "courseID", "courseOrgID", "issuedCertificateCount", "courseStatus", "resourceCount", "resourcesConsumed", "rawCompletionPercentage")
    generateReport(mdoReportDF, reportPath, "mdoid","ConsumptionReport")
    // to be removed once new security job is created
    if (conf.reportSyncEnable) {
      syncReports(s"${conf.localReportDir}/${reportPath}", reportPath)
    }

    val warehouseDF = df
      .withColumn("certificate_generated_on",date_format(from_utc_timestamp(to_utc_timestamp(to_timestamp(
        col("certificateGeneratedOn"), "yyyy-MM-dd'T'HH:mm:ss.SSS"), "UTC"), "IST"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("data_last_generated_on", currentDateTime)
      .select(
        col("userID").alias("user_id"),
        col("batchID").alias("batch_id"),
        col("courseID").alias("content_id"),
        col("enrolledOn").alias("enrolled_on"),
        col("completionPercentage").alias("content_progress_percentage"),
        col("courseProgress").alias("resource_count_consumed"),
        col("userCourseCompletionStatus").alias("user_consumption_status"),
        col("firstCompletedOn").alias("first_completed_on"),
        col("firstCompletedOn").alias("first_certificate_generated_on"),
        col("completedOn").alias("last_completed_on"),
        col("certificate_generated_on").alias("last_certificate_generated_on"),
        col("lastContentAccessTimestamp").alias("content_last_accessed_on"),
        col("Certificate_Generated").alias("certificate_generated"),
        col("issuedCertificateCount").alias("number_of_certificate"),
        col("userRating").alias("user_rating"),
        col("Certificate_ID").alias("certificate_id"),
        col("live_cbp_plan_mandate"),
        col("data_last_generated_on")
      ).dropDuplicates("user_id","batch_id","content_id")
    generateReport(warehouseDF.coalesce(1), s"${reportPath}-warehouse")

    Redis.closeRedisConnect()

  }
}

