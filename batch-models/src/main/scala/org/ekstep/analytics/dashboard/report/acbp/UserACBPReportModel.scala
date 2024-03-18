package org.ekstep.analytics.dashboard.report.acbp

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig, Redis}
import org.ekstep.analytics.framework.FrameworkContext


object UserACBPReportModel extends AbsDashboardModel {
  implicit val className: String = "org.ekstep.analytics.dashboard.report.acbp.UserACBPReportModel"
  override def name() = "UserACBPReportModel"

  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    val today = getDate()

    // get user and org data frames
    var (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()
    val orgHierarchyData = orgHierarchyDataframe()

    val userDataDF = userOrgDF
      .join(orgHierarchyData, Seq("userOrgID"), "left")
      .withColumn("designation", coalesce(col("professionalDetails.designation"), lit("")))
      .withColumn("group", coalesce(col("professionalDetails.group"), lit("")))
      .withColumn("userPrimaryEmail", col("personalDetails.primaryEmail"))
      .withColumn("userMobile", col("personalDetails.mobile"))
      .select("userID", "fullName", "userPrimaryEmail", "userMobile", "userOrgID", "ministry_name", "dept_name", "userOrgName", "designation", "group")
    show(userDataDF, "userDataDF")

    // get course details and course enrolment data frames
    val hierarchyDF = contentHierarchyDataFrame()
    val allCourseProgramESDF = allCourseProgramESDataFrame(Seq("Course", "Program", "Blended Program", "Curated Program", "Standalone Assessment"))
    val allCourseProgramDetailsWithCompDF = allCourseProgramDetailsWithCompetenciesJsonDataFrame(allCourseProgramESDF, hierarchyDF, orgDF)
    val allCourseProgramDetailsDF = allCourseProgramDetailsDataFrame(allCourseProgramDetailsWithCompDF)
    val userCourseProgramEnrolmentDF = userCourseProgramCompletionDataFrame()

    // get ACBP details data frame
    val acbpDF = acbpDetailsDF()

   // CustomUser
    val acbpCustomUserAllotmentDF = acbpDF
      .filter(col("assignmentType") === "CustomUser")
      .withColumn("userID", explode(col("assignmentTypeInfo")))
      .join(userDataDF, Seq("userID", "userOrgID"), "left")
    show(acbpCustomUserAllotmentDF, "acbpCustomUserAllotmentDF")

    // Designation
    val acbpDesignationAllotmentDF = acbpDF
      .filter(col("assignmentType") === "Designation")
      .withColumn("designation", explode(col("assignmentTypeInfo")))
      .join(userDataDF, Seq("userOrgID", "designation"), "left")
    show(acbpDesignationAllotmentDF, "acbpDesignationAllotmentDF")

    // All User
    val acbpAllUserAllotmentDF = acbpDF
      .filter(col("assignmentType") === "AllUser")
      .join(userDataDF, Seq("userOrgID"), "left")
    show(acbpAllUserAllotmentDF, "acbpAllUserAllotmentDF")

    // union of all the response dfs
    val acbpAllotmentDF = Seq(acbpCustomUserAllotmentDF, acbpDesignationAllotmentDF, acbpAllUserAllotmentDF).map(df => {
      df.select("userID", "fullName", "userPrimaryEmail", "userMobile", "designation", "group", "userOrgID", "ministry_name", "dept_name", "userOrgName", "acbpID", "assignmentType", "completionDueDate", "allocatedOn", "acbpCourseIDList")
    }).reduce((a, b) => a.union(b))
    show(acbpAllotmentDF, "acbpAllotmentDF")

    // replace content list with names of the courses instead of ids
    val acbpAllEnrolmentDF = acbpAllotmentDF
      .withColumn("courseID", explode(col("acbpCourseIDList")))
      .join(allCourseProgramDetailsDF, Seq("courseID"), "left")
      .join(userCourseProgramEnrolmentDF, Seq("courseID", "userID"), "left")
      .na.drop(Seq("userID", "courseID"))
      .drop("acbpCourseIDList")
    show(acbpAllEnrolmentDF, "acbpAllEnrolmentDF")

    // for particular userID and course ID, choose allotment entries based on priority rules
    val acbpEnrolmentDF = acbpAllEnrolmentDF
      .groupByLimit(Seq("userID", "courseID"), "completionDueDate", 1, desc = true)
    show(acbpEnrolmentDF, "acbpEnrolmentDF")
    kafkaDispatch(withTimestamp(acbpEnrolmentDF, timestamp), conf.acbpEnrolmentTopic)

    // for enrolment report
    val enrolmentReportDataDF = acbpEnrolmentDF
      .withColumn("currentProgress", expr("CASE WHEN dbCompletionStatus=2 THEN 'Completed' WHEN dbCompletionStatus=1 THEN 'In Progress' WHEN dbCompletionStatus=0 THEN 'Not Started' ELSE 'Not Enrolled' END"))
      .withColumn("courseCompletedTimestamp",  date_format(col("courseCompletedTimestamp"), "dd/MM/yy"))
      .withColumn("allocatedOn",  date_format(col("allocatedOn"), "dd/MM/yy"))
      .withColumn("completionDueDate",  date_format(col("completionDueDate"), "dd/MM/yy"))
      .na.fill("")
    show(enrolmentReportDataDF, "enrolmentReportDataDF")

    val enrolmentReportDF = enrolmentReportDataDF
      .select(
        col("fullName").alias("Name"),
        col("userPrimaryEmail").alias("Email"),
        col("userMobile").alias("Phone"),
        col("ministry_name").alias("Ministry"),
        col("dept_name").alias("Department"),
        col("userOrgName").alias("Organization"),
        col("group").alias("Group"),
        col("designation").alias("Designation"),
        col("courseName").alias("Name of CBP Allocated Course"),
        col("allocatedOn").alias("Allocated On"),
        col("currentProgress").alias("Current Progress"),
        col("completionDueDate").alias("Due Date of Completion"),
        col("courseCompletedTimestamp").alias("Actual Date of Completion"),
        col("userOrgID").alias("mdoid"),
        date_format(current_timestamp(), "dd/MM/yyyy HH:mm:ss a").alias("Report_Last_Generated_On")
      )
      .repartition(1)  // repartitioning here resolves a memory issue
    show(enrolmentReportDF, "enrolmentReportDF")

    val reportPath = s"${conf.acbpReportPath}/${today}"
    generateReport(enrolmentReportDF.drop("mdoid"), s"${reportPath}/CBPEnrollmentReport", fileName="CBPEnrollmentReport")
    generateReport(enrolmentReportDF,  s"${conf.acbpMdoEnrolmentReportPath}/${today}","mdoid", "CBPEnrollmentReport")
    // to be removed once new security job is created
    if (conf.reportSyncEnable) {
      syncReports(s"${conf.localReportDir}/${reportPath}", s"${conf.acbpMdoEnrolmentReportPath}/${today}")
    }

    // for user summary report
    val userSummaryDataDF = acbpEnrolmentDF
      .withColumn("completionDueDateLong", expr("completionDueDate + INTERVAL 24 HOURS").cast(LongType))
      .withColumn("courseCompletedTimestampLong", col("courseCompletedTimestamp").cast(LongType))
      .groupBy("userID", "fullName", "userPrimaryEmail", "userMobile", "designation", "group", "userOrgID", "ministry_name", "dept_name", "userOrgName")
      .agg(
        count("courseID").alias("allocatedCount"),
        expr("SUM(CASE WHEN dbCompletionStatus=2 THEN 1 ELSE 0 END)").alias("completedCount"),
        expr("SUM(CASE WHEN dbCompletionStatus=2 AND courseCompletedTimestampLong<completionDueDateLong THEN 1 ELSE 0 END)").alias("completedBeforeDueDateCount")
      )
    show(userSummaryDataDF, "userSummaryDataDF")

    val userSummaryReportDF = userSummaryDataDF
      .select(
        col("fullName").alias("Name"),
        col("userPrimaryEmail").alias("Email"),
        col("userMobile").alias("Phone"),
        col("ministry_name").alias("Ministry"),
        col("dept_name").alias("Department"),
        col("userOrgName").alias("Organization"),
        col("group").alias("Group"),
        col("designation").alias("Designation"),
        col("allocatedCount").alias("Number of CBP Courses Allocated"),
        col("completedCount").alias("Number of CBP Courses Completed"),
        col("completedBeforeDueDateCount").alias("Number of CBP Courses Completed within due date"),
        col("userOrgID").alias("mdoid"),
        date_format(current_timestamp(), "dd/MM/yyyy HH:mm:ss a").alias("Report_Last_Generated_On")
      )
    show(userSummaryReportDF, "userSummaryReportDF")
    generateReport(userSummaryReportDF.drop("mdoid"), s"${reportPath}/CBPUserSummaryReport", fileName="CBPUserSummaryReport")
    generateReport(userSummaryReportDF.coalesce(1),  s"${conf.acbpMdoSummaryReportPath}/${today}","mdoid", "CBPUserSummaryReport")
    // to be removed once new security job is created
    if(conf.reportSyncEnable) {
      syncReports(s"${conf.localReportDir}/${reportPath}", s"${conf.acbpMdoSummaryReportPath}/${today}")
    }
    syncReports(s"${conf.localReportDir}/${reportPath}", reportPath)

    Redis.closeRedisConnect()
  }
}

