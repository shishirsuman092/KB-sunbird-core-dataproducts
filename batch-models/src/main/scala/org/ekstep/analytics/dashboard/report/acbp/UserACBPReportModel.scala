package org.ekstep.analytics.dashboard.report.acbp

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.ekstep.analytics.dashboard.{DashboardConfig, DummyInput, DummyOutput, Redis}
import org.ekstep.analytics.framework.{FrameworkContext, IBatchModelTemplate}
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._


object UserACBPReportModel extends IBatchModelTemplate[String, DummyInput, DummyOutput, DummyOutput] with Serializable {
  implicit val className: String = "org.ekstep.analytics.dashboard.report.acbp.UserACBPReportModel"
  override def name() = "UserACBPReportModel"
  /**
   * Pre processing steps before running the algorithm. Few pre-process steps are
   * 1. Transforming input - Filter/Map etc.
   * 2. Join/fetch data from LP
   * 3. Join/Fetch data from Cassandra
   */
  override def preProcess(events: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyInput] = {
    val executionTime = System.currentTimeMillis()
    sc.parallelize(Seq(DummyInput(executionTime)))
  }

  /**
   * Method which runs the actual algorithm
   */
  override def algorithm(events: RDD[DummyInput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    val timestamp = events.first().timestamp // extract timestamp from input
    implicit val spark: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()
    processData(config)
    sc.parallelize(Seq()) // return empty rdd
  }

  /**
   * Post processing on the algorithm output. Some of the post processing steps are
   * 1. Saving data to Cassandra
   * 2. Converting to "MeasuredEvent" to be able to dispatch to Kafka or any output dispatcher
   * 3. Transform into a structure that can be input to another data product
   */
  override def postProcess(events: RDD[DummyOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    sc.parallelize(Seq())
  }

  def contentDataFrames2(orgDF: DataFrame, primaryCategories: Seq[String] = Seq("Course", "Program"), runValidation: Boolean = true)(implicit spark: SparkSession, conf: DashboardConfig): (DataFrame, DataFrame, DataFrame) = {
    val allowedCategories = Seq("Course", "Program", "Blended Program", "CuratedCollections", "Standalone Assessment","Curated Program")
    val notAllowed = primaryCategories.toSet.diff(allowedCategories.toSet)
    if (notAllowed.nonEmpty) {
      throw new Exception(s"Category not allowed: ${notAllowed.mkString(", ")}")
    }

    val hierarchyDF = contentHierarchyDataFrame()
    val allCourseProgramESDF = allCourseProgramESDataFrame(primaryCategories)
    val allCourseProgramDetailsWithCompDF = allCourseProgramDetailsWithCompetenciesJsonDataFrame(allCourseProgramESDF, hierarchyDF, orgDF)
    val allCourseProgramDetailsDF = allCourseProgramDetailsDataFrame(allCourseProgramDetailsWithCompDF)

    (hierarchyDF, allCourseProgramDetailsWithCompDF, allCourseProgramDetailsDF)
  }

  def processData(config: Map[String, AnyRef])(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    // parse model config
    println(config)
    implicit val conf: DashboardConfig = parseConfig(config)
    if (conf.debug == "true") debug = true // set debug to true if explicitly specified in the config
    if (conf.validation == "true") validation = true // set validation to true if explicitly specified in the config
    val today = getDate()

    val orgDF = orgDataFrame()
    val userDataDF = userProfileDetailsDF(orgDF)
      .withColumn("designation", coalesce(col("professionalDetails.designation"), lit("")))
      .withColumn("group", coalesce(col("professionalDetails.group"), lit("")))
      .select("userID", "fullName", "maskedEmail", "maskedPhone", "userOrgID", "userOrgName", "designation", "group")
    show(userDataDF, "userDataDF")

    val (hierarchyDF, allCourseProgramDetailsWithCompDF, allCourseProgramDetailsDF) = contentDataFrames2(orgDF)
    val userCourseProgramEnrolmentDF = userCourseProgramCompletionDataFrame()

    val acbpDF = acbpDetailsDF()

    /*
    * TODO:
    *  - explode vs explode_outer
    *  - primary categories
    *  - report generated on
    * */
   // CustomUser
    val acbpCustomUserDF = acbpDF
      .filter(col("assignmentType") === "CustomUser")
      .withColumn("userID", explode(col("assignmentTypeInfo")))
      .join(userDataDF, Seq("userID", "userOrgID"), "left")
    show(acbpCustomUserDF, "acbpCustomUserDF")

    // Designation
    val acbpDesignationDF = acbpDF
      .filter(col("assignmentType") === "Designation")
      .withColumn("designation", explode(col("assignmentTypeInfo")))
      .join(userDataDF, Seq("userOrgID", "designation"), "inner")
    show(acbpDesignationDF, "acbpDesignationDF")

    // All User
    val acbpAllUserDF = acbpDF
      .filter(col("assignmentType") === "AllUser")
      .join(userDataDF, Seq("userOrgID"), "inner")
      .dropDuplicates("userID") // TODO dropDuplicates
    show(acbpAllUserDF, "acbpAllUserDF")

    // union of all the response dfs
    val acbpJoinedDF = acbpCustomUserDF.union(acbpDesignationDF).union(acbpAllUserDF)
    show(acbpJoinedDF, "acbpJoinedDF")

    // replace content list with names of the courses instead of ids
    val finalResultDF = acbpJoinedDF
      .withColumn("courseID", explode(col("acbpCourseIDList")))
      .join(allCourseProgramDetailsDF, Seq("courseID"), "left")
      .join(userCourseProgramEnrolmentDF, Seq("courseID", "userID"), "left")
    show(finalResultDF, "finalResultDF")

    // for enrollment report
    val enrollmentDataDF = finalResultDF  // TODO completionDueDate
      .groupBy("userID", "fullName", "maskedEmail", "maskedPhone", "designation", "group", "userOrgID", "userOrgName", "completionDueDate", "allocatedOn")
      .agg(
        collect_list("courseName").alias("acbpCourseNameList"),
        countDistinct("dbCompletionStatus").alias("distinctStatusCount"),
        max("dbCompletionStatus").alias("maxStatus"),
        max("courseCompletedTimestamp").alias("maxCourseCompletedTimestamp")
      )
      .withColumn("currentProgress", expr("CASE WHEN distinctStatusCount=1 AND maxStatus=2 THEN 'Completed' WHEN distinctStatusCount=1 AND maxStatus=1 THEN 'In Progress' WHEN distinctStatusCount=1 AND maxStatus=0 THEN 'Not Started' WHEN distinctStatusCount>1 THEN 'In Progress' ELSE 'Not Enrolled' END"))
      .withColumn("completionDate", expr("CASE WHEN distinctStatusCount=1 AND maxStatus=2 THEN maxCourseCompletedTimestamp END"))
      .na.fill("")
    show(enrollmentDataDF, "enrollmentDataDF")

    val enrollmentReportDF = enrollmentDataDF
      .select(
        col("fullName").alias("Name"),
        col("maskedEmail").alias("Masked Email"),
        col("maskedPhone").alias("Masked Phone"),
        col("userOrgName").alias("MDO Name"),
        col("group").alias("Group"),
        col("designation").alias("Designation"),
        concat_ws(",", col("acbpCourseNameList")).alias("Name of the ACBP Allocated Courses"),
        col("allocatedOn").alias("Allocated On"),
        col("currentProgress").alias("Current Progress"),
        col("completionDueDate").alias("Due Date of Completion"),
        col("completionDate").alias("Actual Date of Completion")
      )
    show(enrollmentReportDF, "enrollmentReportDF")

    // for user summary report
    val userSummaryDataDF = finalResultDF
      .withColumn("completionDueDate", col("completionDueDate").cast(LongType))
      .groupBy("userID", "fullName", "maskedEmail", "maskedPhone", "designation", "group", "userOrgID", "userOrgName")
      .agg(
        count("courseID").alias("allocatedCount"),
        expr("SUM(CASE WHEN dbCompletionStatus=2 THEN 1 ELSE 0 END)").alias("completedCount"),
        expr("SUM(CASE WHEN dbCompletionStatus=2 AND courseCompletedTimestamp<=completionDueDate THEN 1 ELSE 0 END)").alias("completedBeforeDueDateCount")
      )

    val userSummaryReportDF = userSummaryDataDF
      .select(
        col("fullName").alias("Name"),
        col("maskedEmail").alias("Masked Email"),
        col("maskedPhone").alias("Masked Phone"),
        col("userOrgName").alias("MDO Name"),
        col("group").alias("Group"),
        col("designation").alias("Designation"),
        col("allocatedCount").alias("Number of ACBP Courses Allocated"),
        col("completedCount").alias("Number of ACBP Courses Completed"),
        col("completedBeforeDueDateCount").alias("Number of ACBP Courses Completed within due date")
      )
    show(userSummaryReportDF, "userSummaryReportDF")

    //val reportPath = s"${conf.userReportPath}/${today}"
    val enrollmentReportPath = s"standalone-reports/acbp-enrollment-exhaust/${today}"
    generateReportsWithoutPartition(enrollmentReportDF, enrollmentReportPath, "ACBPEnrollmentReport")

    //val reportPath = s"${conf.userReportPath}/${today}"
    val userReportPath = s"standalone-reports/acbp-user-summary-exhaust/${today}"
    generateReportsWithoutPartition(userSummaryReportDF, userReportPath, "ACBPUserSummaryReport")

    syncReports(s"/tmp/${enrollmentReportPath}", enrollmentReportPath)
    syncReports(s"/tmp/${userReportPath}", userReportPath)

    Redis.closeRedisConnect()

  }
}