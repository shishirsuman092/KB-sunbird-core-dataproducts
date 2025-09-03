package org.ekstep.analytics.adhoc

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.ekstep.analytics.dashboard.DashboardUtil.{getDate, warehouseCache}
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig}
import org.ekstep.analytics.framework.FrameworkContext

object ProgramProgressIssue extends AbsDashboardModel{

  implicit val className: String = "org.ekstep.analytics.adhoc.ProgramProgressIssue"

  override def name() = "ProgramProgressIssue"

  /**
   * Master method, does all the work, fetching, processing and dispatching
   *
   * @param timestamp unique timestamp from the start of the processing
   */
  override def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {

    val today = getDate()

    //GET ORG DATA
    val (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()
    val orgHierarchyData = orgHierarchyDataframe()
    val userDataDF = userOrgDF
      .join(broadcast(orgHierarchyData), Seq("userOrgID"), "left")
      .withColumn("designation", coalesce(col("professionalDetails.designation"), lit("")))

    // Get course data first
    //val allCourseProgramDetailsDF = contentWithOrgDetailsDataFrame(orgDF, Seq("Course","Program","Blended Program","Curated Program","Moderated Course"))
    val allProgramDetailsDF = contentWithOrgDetailsDataFrame(orgDF, Seq("Program", "Curated Program"))
    val allCourseDetailsDF = contentWithOrgDetailsDataFrame(orgDF, Seq("Course","Curated Program","Moderated Course"))
    // enrolment data
    val enrolmentDF = userCourseProgramCompletionDataFrame()
    // content resource data
    val contentResourceDF = warehouseCache.load(conf.dwContentResourceTable)
    // all user program progress
    val allProgramCompletionWithDetailsDF = allCourseProgramCompletionWithDetailsDataFrame(enrolmentDF, allProgramDetailsDF, userDataDF)
    val allCourseCompletionWithDetailsDF = allCourseProgramCompletionWithDetailsDataFrame(enrolmentDF, allCourseDetailsDF, userDataDF)
    // filter all program that has user consumption at 100% but certificate is not generated
    val programWithConsumptionButNoCert = allProgramCompletionWithDetailsDF.filter(col("userCourseCompletionStatus") === "completed" and (col("certificateID") isNull))

    programWithConsumptionButNoCert.printSchema()
    programWithConsumptionButNoCert.select(col("courseID")).distinct().show(false)
    println(programWithConsumptionButNoCert.select(col("courseID")).distinct().count())

    val programIDs = programWithConsumptionButNoCert.select(col("courseID").alias("programID")).distinct()
    val programResources = programIDs.join(contentResourceDF, programIDs("programID") === contentResourceDF("content_id"), "inner")

    programResources.printSchema()
    programResources.distinct().show(false)
    println(programResources.select(col("resource_id")).distinct().count())

    val filteredProgramResourcesDF =programResources.select(
      col("programID"),
      col("resource_id"),
      col("resource_name"),
      col("resource_type"),
      col("resource_duration")
    )

    val allResourceConsumptionForFilteredProgramsDF = allCourseCompletionWithDetailsDF
      .join(filteredProgramResourcesDF, allProgramCompletionWithDetailsDF("courseID") === filteredProgramResourcesDF("resource_id"), "inner")
      .select(
        col("userID"),
        col("courseID").alias("resourceID"),
        col("batchID"),
        col("programID"),
        col("courseName"),
        col("courseProgress"),
        col("dbCompletionStatus"),
        col("courseEnrolledTimestamp"),
        col("courseCompletedTimestamp"),
        col("lastContentAccessTimestamp"),
        col("issuedCertificateCount"),
        col("firstCompletedOn"),
        col("certificateGeneratedOn"),
        col("courseStatus")
      )

    println("all records ".concat(allResourceConsumptionForFilteredProgramsDF.count().toString))
    println("distinct user records ".concat(allResourceConsumptionForFilteredProgramsDF.select(col("userID")).distinct().count().toString))

    allResourceConsumptionForFilteredProgramsDF.coalesce(1).write.mode(SaveMode.Overwrite).format("csv").option("header", true).save(s"/tmp/${today}-resource-program-progress")
    programWithConsumptionButNoCert.coalesce(1).write.mode(SaveMode.Overwrite).format("csv").option("header", true).save(s"/tmp/${today}-program-progress")

  }
}
