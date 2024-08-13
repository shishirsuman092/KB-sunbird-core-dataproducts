package org.ekstep.analytics.dashboard.report.blended

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig, Redis}
import org.ekstep.analytics.framework.FrameworkContext


object BlendedProgramReportModel extends AbsDashboardModel {

  implicit val className: String = "org.ekstep.analytics.dashboard.report.blended.BlendedProgramReportModel"

  override def name() = "BlendedProgramReportModel"

  /**
   * Master method, does all the work, fetching, processing and dispatching
   *
   * @param timestamp unique timestamp from the start of the processing
   */
  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    val today = getDate()

    // get user and user org data
    var (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()
    val orgHierarchyData = orgHierarchyDataframe()

    var userDataDF = userOrgDF
      .withColumn("userPrimaryEmail", col("personalDetails.primaryEmail"))
      .withColumn("userMobile", col("personalDetails.mobile"))
      .withColumn("userGender", col("personalDetails.gender"))
      .withColumn("userCategory", col("personalDetails.category"))
      .withColumn("userDesignation", col("professionalDetails.designation"))
      .withColumn("userGroup", col("professionalDetails.group"))
      .withColumn("userTags", concat_ws(", ", col("additionalProperties.tag")))
      .drop("personalDetails", "professionalDetails", "additionalProperties")
    show(userDataDF, "userDataDF -1")

    userDataDF = userDataDF
      .join(orgHierarchyData, Seq("userOrgID"), "left")
    show(userDataDF, "userDataDF")

    // Get Blended Program data
    val blendedProgramESDF = contentESDataFrame(Seq("Blended Program"), "bp")
      .where(expr("bpStatus IN ('Live', 'Retired')"))
      .where(col("bpLastPublishedOn").isNotNull)
    val bpOrgDF = orgDF.select(
      col("orgID").alias("bpOrgID"),
      col("orgName").alias("bpOrgName"),
      col("orgCreatedDate").alias("bpOrgCreatedDate")
    )
    val bpWithOrgDF = blendedProgramESDF.join(bpOrgDF, Seq("bpOrgID"), "left")
    show(bpWithOrgDF, "bpWithOrgDF")

    // add BP batch info
    val (bpBatchDF, bpBatchSessionDF) = bpBatchDataFrame()

    var bpWithBatchDF = bpWithOrgDF
      .join(bpBatchDF, Seq("bpID"), "left")
    show(bpWithBatchDF, "bpWithBatchDF")

    val batchCreatedByDF = userDataDF.select(
      col("userID").alias("bpBatchCreatedBy"),
      col("fullName").alias("bpBatchCreatedByName")
    )
    bpWithBatchDF = bpWithBatchDF.join(batchCreatedByDF, Seq("bpBatchCreatedBy"), "left")

    // add BP user progress info

    // get enrolment table
    val userEnrolmentDF = userCourseProgramCompletionDataFrame(extraCols = Seq("courseContentStatus"))
    show(userEnrolmentDF, "userEnrolmentDF")

    val bpUserEnrolmentDF = userEnrolmentDF.select(
      col("userID"),
      col("courseID").alias("bpID"),
      col("batchID").alias("bpBatchID"),
      col("courseEnrolledTimestamp").alias("bpEnrolledTimestamp"),
      col("issuedCertificateCount").alias("bpIssuedCertificateCount"),
      col("courseContentStatus").alias("bpContentStatus"),
      col("dbCompletionStatus").alias("bpUserCompletionStatus"),
      col("lastContentAccessTimestamp").alias("bpLastContentAccessTimestamp"),
      col("courseCompletedTimestamp").alias("bpCompletedTimestamp")
    )
    show(bpUserEnrolmentDF, "bpUserEnrolmentDF")

    val bpCompletionDF = bpWithBatchDF.join(bpUserEnrolmentDF.drop("bpContentStatus"), Seq("bpID", "bpBatchID"), "inner")
    show(bpCompletionDF, "bpCompletionDF")

    // get content status DF
    val bpUserContentStatusDF = bpUserEnrolmentDF
      .select(col("userID"), col("bpID"), col("bpBatchID"), explode_outer(col("bpContentStatus")))
      .withColumnRenamed("key", "bpChildID")
      .withColumnRenamed("value", "bpContentStatus")
    show(bpUserContentStatusDF, "bpUserContentStatusDF")

    // add user and user org info
    val bpCompletionWithUserDetailsDF = bpCompletionDF.join(userDataDF, Seq("userID"), "left")
    show(bpCompletionWithUserDetailsDF, "bpCompletionWithUserDetailsDF")

    // children
    val hierarchyDF = contentHierarchyDataFrame()
    show(hierarchyDF, "hierarchyDF")

    val bpChildDF = bpChildDataFrame(blendedProgramESDF, hierarchyDF)

    // add children info to bpCompletionWithUserDetailsDF
    val bpCompletionWithChildrenDF = bpCompletionWithUserDetailsDF.join(bpChildDF, Seq("bpID"), "left")
      .withColumn("bpChildMode", expr("CASE WHEN LOWER(bpChildCategory) = 'offline session' THEN 'Offline' ELSE '' END"))
      .join(bpBatchSessionDF, Seq("bpID", "bpBatchID", "bpChildID"), "left")
    show(bpCompletionWithChildrenDF, "bpCompletionWithChildrenDF")

    // add children batch info
    val bpChildBatchDF = bpBatchDF.select(
      col("bpID").alias("bpChildID"),
      col("bpBatchID").alias("bpChildBatchID"),
      col("bpBatchName").alias("bpChildBatchName"),
      col("bpBatchStartDate").alias("bpChildBatchStartDate"),
      col("bpBatchEndDate").alias("bpChildBatchEndDate"),
      col("bpBatchLocation").alias("bpChildBatchLocation"),
      col("bpBatchCurrentSize").alias("bpChildBatchCurrentSize")
    )
    show(bpChildBatchDF, "bpChildBatchDF")

    val bpChildBatchSessionDF =  bpBatchSessionDF.select(
      col("bpChildID").alias("bpChildChildID"),
      col("bpID").alias("bpChildID"),
      col("bpBatchID").alias("bpChildBatchID"),
      col("bpBatchSessionType").alias("bpChildBatchSessionType"),
      col("bpBatchSessionFacilators").alias("bpChildBatchSessionFacilators"),
      col("bpBatchSessionStartDate").alias("bpChildBatchSessionStartDate"),
      col("bpBatchSessionStartTime").alias("bpChildBatchSessionStartTime"),
      col("bpBatchSessionEndTime").alias("bpChildBatchSessionEndTime")
    )
    show(bpChildBatchSessionDF, "bpChildBatchSessionDF")

    val relevantChildBatchInfoDF = bpChildDF.select("bpChildID")
      .join(bpChildBatchDF, Seq("bpChildID"), "left")
      .join(bpChildBatchSessionDF, Seq("bpChildID", "bpChildBatchID"), "left")
      .select("bpChildID", "bpChildBatchID", "bpChildBatchName", "bpChildBatchStartDate", "bpChildBatchEndDate", "bpChildBatchLocation", "bpChildBatchCurrentSize", "bpChildBatchSessionType", "bpChildBatchSessionFacilators")
    show(relevantChildBatchInfoDF, "relevantChildBatchInfoDF")

    val bpCompletionWithChildBatchInfoDF = bpCompletionWithChildrenDF.join(relevantChildBatchInfoDF, Seq("bpChildID"), "left")
    show(bpCompletionWithChildBatchInfoDF, "bpCompletionWithChildBatchInfoDF")

    // add child progress info
    val bpChildUserEnrolmentDF = userEnrolmentDF.select(
      col("userID"),
      col("courseID").alias("bpChildID"),
      col("courseProgress").alias("bpChildProgress"),
      col("dbCompletionStatus").alias("bpChildUserStatus"),
      col("lastContentAccessTimestamp").alias("bpChildLastContentAccessTimestamp"),
      col("courseCompletedTimestamp").alias("bpChildCompletedTimestamp")
    )
    show(bpChildUserEnrolmentDF, "bpChildUserEnrolmentDF")

    var bpChildrenWithProgress = bpCompletionWithChildBatchInfoDF.join(bpChildUserEnrolmentDF, Seq("userID", "bpChildID"), "left")
      .na.fill(0, Seq("bpChildResourceCount", "bpChildProgress"))
    show(bpChildrenWithProgress, "bpChildrenWithProgress -2")

    // add content status from map
    bpChildrenWithProgress = bpChildrenWithProgress
      .join(bpUserContentStatusDF, Seq("userID", "bpID", "bpChildID", "bpBatchID"), "left")
      .withColumn("bpChildUserStatus", coalesce(col("bpChildUserStatus"), col("bpContentStatus")))
      .drop("bpContentStatus")

    bpChildrenWithProgress = bpChildrenWithProgress
      .withColumn("completionPercentage", expr("CASE WHEN bpChildUserStatus=2 THEN 100.0 WHEN bpChildProgress=0 OR bpChildResourceCount=0 OR bpChildUserStatus=0 THEN 0.0 ELSE 100.0 * bpChildProgress / bpChildResourceCount END"))
      .withColumn("completionPercentage", expr("CASE WHEN completionPercentage > 100.0 THEN 100.0 WHEN completionPercentage < 0.0 THEN 0.0 ELSE completionPercentage END"))
      .withColumnRenamed("completionPercentage", "bpChildProgressPercentage")
    show(bpChildrenWithProgress, "bpChildrenWithProgress -1")

    bpChildrenWithProgress = bpChildrenWithProgress
      .withColumn("bpChildAttendanceStatus", expr("CASE WHEN bpChildUserStatus=2 THEN 'Attended' ELSE 'Not Attended' END"))
      .withColumn("bpChildOfflineAttendanceStatus", expr("CASE WHEN bpChildMode='Offline' THEN bpChildAttendanceStatus ELSE '' END"))
    show(bpChildrenWithProgress, "bpChildrenWithProgress")

    // finalize report data frame
    val fullDF = bpChildrenWithProgress
      .withColumn("bpEnrolledOn", to_date(col("bpEnrolledTimestamp"), "dd/MM/yyyy"))
      .withColumn("bpBatchStartDate", to_date(col("bpBatchStartDate"), "dd/MM/yyyy"))
      .withColumn("bpBatchEndDate", to_date(col("bpBatchEndDate"), "dd/MM/yyyy"))
      .withColumn("bpChildBatchStartDate", to_date(col("bpChildBatchStartDate"), "dd/MM/yyyy"))
      .withColumn("bpChildCompletedTimestamp", to_date(col("bpChildBatchStartDate"), "dd/MM/yyyy"))
      .withColumn("bpChildCompletedOn", expr("CASE WHEN bpChildMode='Offline' AND bpChildUserStatus=2 THEN bpBatchSessionStartDate ELSE bpChildCompletedTimestamp END"))
      .withColumn("bpChildLastContentAccessTimestamp", to_date(col("bpChildLastContentAccessTimestamp"), "dd/MM/yyyy"))
      .withColumn("bpChildLastAccessedOn", expr("CASE WHEN bpChildMode='Offline' THEN bpBatchSessionStartDate ELSE bpChildLastContentAccessTimestamp END"))
      .withColumn("bpChildProgressPercentage", round(col("bpChildProgressPercentage"), 2))
      .withColumn("Report_Last_Generated_On", date_format(current_timestamp(), "dd/MM/yyyy HH:mm:ss a"))
      .withColumn("Certificate_Generated", expr("CASE WHEN bpIssuedCertificateCount > 0 THEN 'Yes' ELSE 'No' END"))
      .withColumn("bpChildOfflineStartDate", expr("CASE WHEN bpChildMode='Offline' THEN bpBatchSessionStartDate ELSE '' END"))
      .withColumn("bpChildOfflineStartTime", expr("CASE WHEN bpChildMode='Offline' THEN bpBatchSessionStartTime ELSE '' END"))
      .withColumn("bpChildOfflineEndTime", expr("CASE WHEN bpChildMode='Offline' THEN bpBatchSessionEndTime ELSE '' END"))
      .withColumn("bpChildUserStatus", expr("CASE WHEN bpChildUserStatus=2 THEN 'Completed' ELSE 'Not Completed' END"))
      .durationFormat("bpChildDuration")
      .distinct()
    show(fullDF, "fullDF")

    val fullReportDF = fullDF
      .select(
        col("userID"),
        col("userOrgID"),
        col("bpID"),
        col("bpOrgID"),
        col("bpChildID"),
        col("bpBatchID"),
        col("bpIssuedCertificateCount"),
        col("fullName").alias("Name"),
        col("userPrimaryEmail").alias("Email"),
        col("userMobile").alias("Phone_Number"),
        col("maskedEmail"),
        col("maskedPhone"),
        col("userDesignation").alias("Designation"),
        col("userGroup").alias("Group"),
        col("userGender").alias("Gender"),
        col("userCategory").alias("Category"),
        col("userTags").alias("Tag"),
        col("ministry_name").alias("Ministry"),
        col("dept_name").alias("Department"),
        col("userOrgName").alias("Organization"),

        col("bpOrgName").alias("Provider_Name"),
        col("bpName").alias("Program_Name"),
        col("bpBatchName").alias("Batch_Name"),
        col("bpBatchLocation").alias("Batch_Location"),
        col("bpBatchStartDate").alias("Batch_Start_Date"),
        col("bpBatchEndDate").alias("Batch_End_Date"),
        col("bpEnrolledOn").alias("Enrolled_On"),

        col("bpChildName").alias("Component_Name"),
        col("bpChildCategory").alias("Component_Type"),
        col("bpChildMode").alias("Component_Mode"),
        col("bpChildUserStatus").alias("Status"),
        col("bpChildDuration").alias("Component_Duration"),
        col("bpChildProgressPercentage").alias("Component_Progress_Percentage"),
        col("bpChildCompletedOn").alias("Component_Completed_On"),
        col("bpChildLastAccessedOn").alias("Last_Accessed_On"),
        col("bpChildOfflineStartDate").alias("Offline_Session_Date"),
        col("bpChildOfflineStartTime").alias("Offline_Session_Start_Time"),
        col("bpChildOfflineEndTime").alias("Offline_Session_End_Time"),
        col("bpChildOfflineAttendanceStatus").alias("Offline_Attendance_Status"),
        col("bpBatchSessionFacilators").alias("Instructor(s)_Name"),
        col("bpBatchCreatedByName").alias("Program_Coordinator_Name"),
        col("bpProgramDirectorName"),
        col("Certificate_Generated"),
        col("userOrgID").alias("mdoid"),
        col("bpID").alias("contentid"),
        col("Report_Last_Generated_On")
      )
      .orderBy("bpID", "userID")
      .coalesce(1)

    show(fullReportDF, "fullReportDF")

    val reportPath = s"${conf.blendedReportPath}/${today}"
    val reportPathMDO = s"${conf.blendedReportPath}-mdo/${today}"
    val reportPathCBP = s"${conf.blendedReportPath}-cbp/${today}"

    // generateReport(fullReportDF, s"${reportPath}-full")
    val reportDF = fullReportDF.drop("userID", "userOrgID", "bpID", "bpOrgID", "bpChildID", "bpBatchID", "bpIssuedCertificateCount", "bpProgramDirectorName")

    // mdo wise
    val mdoReportDF = reportDF.drop("maskedEmail", "maskedPhone", "contentid")
    generateReport(mdoReportDF,  reportPathMDO,"mdoid", "BlendedProgramReport")
    // to be removed once new security job is created
    if (conf.reportSyncEnable) {
      syncReports(s"${conf.localReportDir}/${reportPath}", reportPathMDO)
    }
    // cbp wise
    val cbpReportDF = fullReportDF.drop("userID", "userOrgID", "bpID", "bpChildID", "bpBatchID", "bpIssuedCertificateCount", "bpProgramDirectorName","contentid")
      .drop("Email", "Phone_Number", "mdoid")
      .withColumnRenamed("bpOrgID", "mdoid")
      .withColumnRenamed("maskedEmail", "Email")
      .withColumnRenamed("maskedPhone", "Phone_Number")

    show(cbpReportDF, "cbpReportDF")
    generateAndSyncReports(cbpReportDF, "mdoid", reportPathCBP, "BlendedProgramReport")


    val df_warehouse = fullDF
      .withColumn("data_last_generated_on", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss a"))
      .select(
        col("userID").alias("user_id"),
        col("bpID").alias("content_id"),
        col("bpBatchID").alias("batch_id"),
        col("bpBatchLocation").alias("batch_location"),
        col("bpChildName").alias("component_name"),
        col("bpChildID").alias("component_id"),
        col("bpChildCategory").alias("component_type"),
        col("bpChildBatchSessionType").alias("component_mode"),
        col("bpChildUserStatus").alias("component_status"),
        col("bpChildDuration").alias("component_duration"),
        col("bpChildProgressPercentage").alias("component_progress_percentage"),
        col("bpChildCompletedOn").alias("component_completed_on"),
        col("bpChildLastAccessedOn").alias("last_accessed_on"),
        col("bpChildOfflineStartDate").alias("offline_session_date"),
        col("bpChildOfflineStartTime").alias("offline_session_start_time"),
        col("bpChildOfflineEndTime").alias("offline_session_end_time"),
        col("bpChildAttendanceStatus").alias("offline_attendance_status"),
        col("bpBatchSessionFacilators").alias("instructors_name"),
        col("bpProgramDirectorName").alias("program_coordinator_name"),
        col("data_last_generated_on")
      )

    generateReport(df_warehouse.coalesce(1), s"${reportPath}-warehouse")

    Redis.closeRedisConnect()
  }

  def bpBatchDataFrame()(implicit spark: SparkSession, conf: DashboardConfig): (DataFrame, DataFrame) = {
    val batchDF = courseBatchDataFrame()
    var bpBatchDF = batchDF.select(
        col("courseID").alias("bpID"),
        col("batchID").alias("bpBatchID"),
        col("courseBatchCreatedBy").alias("bpBatchCreatedBy"),
        col("courseBatchName").alias("bpBatchName"),
        col("courseBatchStartDate").alias("bpBatchStartDate"),
        col("courseBatchEndDate").alias("bpBatchEndDate"),
        col("courseBatchAttrs").alias("bpBatchAttrs")
      )
      .withColumn("bpBatchAttrs", from_json(col("bpBatchAttrs"), Schema.batchAttrsSchema))
      .withColumn("bpBatchLocation", col("bpBatchAttrs.batchLocationDetails"))
      .withColumn("bpBatchCurrentSize", col("bpBatchAttrs.currentBatchSize"))
    show(bpBatchDF, "bpBatchDF")

    val bpBatchSessionDF = bpBatchDF.select("bpID", "bpBatchID", "bpBatchAttrs")
      .withColumn("bpBatchSessionDetails", explode_outer(col("bpBatchAttrs.sessionDetails_v2")))
      .withColumn("bpChildID", col("bpBatchSessionDetails.sessionId")) // sessionId contains the value of bpChildID, for some reason that's the name of the column, instead of calling this childID or something useful
      .withColumn("bpBatchSessionType", col("bpBatchSessionDetails.sessionType"))
      .withColumn("bpBatchSessionFacilators", concat_ws(", ", col("bpBatchSessionDetails.facilatorDetails.name")))
      .withColumn("bpBatchSessionStartDate", col("bpBatchSessionDetails.startDate"))
      .withColumn("bpBatchSessionStartTime", col("bpBatchSessionDetails.startTime"))
      .withColumn("bpBatchSessionEndTime", col("bpBatchSessionDetails.endTime"))
      .drop("bpBatchAttrs", "bpBatchSessionDetails")
    show(bpBatchSessionDF, "bpBatchSessionDF")

    bpBatchDF = bpBatchDF.drop("bpBatchAttrs")

    (bpBatchDF, bpBatchSessionDF)
  }

  def bpChildDataFrame(blendedProgramESDF: DataFrame, hierarchyDF: DataFrame)(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    val bpIDsDF = blendedProgramESDF.select("bpID")
    show(bpIDsDF, "bpIDsDF")

    // L1 children with modules (course units)
    val bpChildL1WithModulesDF = addHierarchyColumn(bpIDsDF, hierarchyDF, "bpID", "data", children = true, l2Children = true)
      .withColumn("bpChild", explode_outer(col("data.children")))
      .drop("identifier", "data")
    show(bpChildL1WithModulesDF, "bpChildL1WithModulesDF")

    // L1 children without modules
    val bpChildL1DF = bpChildL1WithModulesDF.where(expr("bpChild.primaryCategory != 'Course Unit'"))
      .withColumn("bpChildID", col("bpChild.identifier"))
      .withColumn("bpChildName", col("bpChild.name"))
      .withColumn("bpChildCategory", col("bpChild.primaryCategory"))
      .withColumn("bpChildDuration", col("bpChild.duration"))
      .withColumn("bpChildResourceCount", col("bpChild.leafNodesCount"))
      .drop("bpChild")
    show(bpChildL1DF, "bpChildL1DF")

    // L2 children (i.e. children of the modules)
    val bpChildL2DF = bpChildL1WithModulesDF.where(expr("bpChild.primaryCategory = 'Course Unit'"))
      .withColumn("bpModuleChild", explode_outer(col("bpChild.children")))
      .drop("bpChild")
      .withColumn("bpChildID", col("bpModuleChild.identifier"))
      .withColumn("bpChildName", col("bpModuleChild.name"))
      .withColumn("bpChildCategory", col("bpModuleChild.primaryCategory"))
      .withColumn("bpChildDuration", col("bpModuleChild.duration"))
      .withColumn("bpChildResourceCount", col("bpModuleChild.leafNodesCount"))
      .drop("bpModuleChild")
    show(bpChildL2DF, "bpChildL2DF")

    // merge L1 and L2 children
    val bpChildDF = bpChildL1DF.union(bpChildL2DF)
    show(bpChildDF, "bpChildDF")

    bpChildDF
  }
}

