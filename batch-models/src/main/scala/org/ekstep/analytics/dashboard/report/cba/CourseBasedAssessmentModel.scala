package org.ekstep.analytics.dashboard.report.cba

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig, Redis}
import org.ekstep.analytics.framework.FrameworkContext

object CourseBasedAssessmentModel extends AbsDashboardModel {

  implicit val className: String = "org.ekstep.analytics.dashboard.report.cba.CourseBasedAssessmentModel"
  override def name() = "CourseBasedAssessmentModel"

  /**
   * Master method, does all the work, fetching, processing and dispatching
   *
   * @param timestamp unique timestamp from the start of the processing
   */
  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    val today = getDate()

    val (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()
    orgDF.cache()
    // get course details, with rating info
    val (hierarchyDF, allCourseProgramDetailsWithCompDF, allCourseProgramDetailsDF,
    allCourseProgramDetailsWithRatingDF) = contentDataFrames(orgDF, Seq("Course", "Program", "Blended Program", "Standalone Assessment", "Curated Program"), runValidation = false)

    val assessmentDF = assessmentESDataFrame(Seq("Course", "Standalone Assessment", "Blended Program"))
    val assessWithHierarchyDF = assessWithHierarchyDataFrame(assessmentDF, hierarchyDF, orgDF).cache()
    val assessWithDetailsDF = assessWithHierarchyDF.drop("children")

    val assessChildrenDF = assessmentChildrenDataFrame(assessWithHierarchyDF)
    val userAssessmentDF = cache.load("userAssessment").filter(col("assessUserStatus") === "SUBMITTED")
      .withColumn("assessStartTime", col("assessStartTimestamp").cast("long"))
      .withColumn("assessEndTime", col("assessEndTimestamp").cast("long"))
    val userAssessChildrenDF = userAssessmentChildrenDataFrame(userAssessmentDF, assessChildrenDF)
    val userAssessChildrenDetailsDF = userAssessmentChildrenDetailsDataFrame(userAssessChildrenDF, assessWithDetailsDF,
      allCourseProgramDetailsWithRatingDF, userOrgDF)

    val orgHierarchyData = orgHierarchyDataframe()

    val userAssessChildDataDF = userAssessChildrenDetailsDF
      .join(broadcast(orgHierarchyData), Seq("userOrgID"), "left")

    val retakesDF = userAssessChildDataDF
      .groupBy("assessChildID", "userID")
      .agg(countDistinct("assessStartTime").alias("retakes"))

    val userAssessChildDataLatestDF = userAssessChildDataDF
      .groupByLimit(Seq("assessChildID", "userID"), "assessEndTimestamp", 1, desc = true)
      .drop("rowNum")
      .join(broadcast(retakesDF), Seq("assessChildID", "userID"), "left")

    val finalDF = userAssessChildDataLatestDF
      .withColumn("userAssessmentDuration", unix_timestamp(col("assessEndTimestamp")) - unix_timestamp(col("assessStartTimestamp")))
      .withColumn("Pass", expr("CASE WHEN assessPass == 1 THEN 'Yes' ELSE 'No' END"))
      .durationFormat("assessExpectedDuration", "totalAssessmentDuration")
      .withColumn("assessPercentage", when(col("assessPassPercentage").isNotNull, col("assessPassPercentage"))
        .otherwise(lit("Need to pass in all sections")))
      .withColumn("assessment_type", when(col("assessCategory") === "Standalone Assessment", col("assessCategory"))
        .when(col("assessPrimaryCategory").isNotNull, col("assessPrimaryCategory"))
        .otherwise(lit("")))
      .withColumn("assessment_course_name", when(col("assessment_type") === "Course Assessment", col("assessName"))
        .otherwise(lit("")))
      .withColumn("Total_Score_Calculated", when(col("assessMaxQuestions").isNotNull, col("assessMaxQuestions") * 1))
      .withColumn("course_id", when(col("assessCategory") === "Standalone Assessment", lit(""))
        .otherwise(col("assessID")))
      .withColumn("Tags", concat_ws(", ", col("additionalProperties.tag")))

    val fullReportDFNew = finalDF
      .select(
        col("userID").alias("User ID"),
        col("assessID"),
        col("assessOrgID"),
        col("assessChildID"),
        col("userOrgID"),
        col("fullName").alias("Full Name"),
        col("professionalDetails.designation").alias("Designation"),
        col("personalDetails.primaryEmail").alias("E mail"),
        col("personalDetails.mobile").alias("Phone Number"),
        col("professionalDetails.group").alias("Group"),
        col("Tags"),
        col("ministry_name").alias("Ministry"),
        col("dept_name").alias("Department"),
        col("userOrgName").alias("Organisation"),
        col("assessChildName").alias("Assessment Name"),
        col("assessment_type").alias("Assessment Type"),
        col("assessOrgName").alias("Assessment/Content Provider"),
        from_unixtime(col("assessLastPublishedOn").cast("long"), dateFormat).alias("Assessment Publish Date"),
        col("assessment_course_name").alias("Course Name"),
        col("course_id").alias("Course ID"),
        col("totalAssessmentDuration").alias("Assessment Duration"),
        from_unixtime(col("assessEndTime"), dateFormat).alias("Last Attempted Date"),
        col("assessOverallResult").alias("Latest Percentage Achieved"),
        col("assessPercentage").alias("Cut off Percentage"),
        col("Pass"),
        col("assessMaxQuestions").alias("Total Questions"),
        col("assessIncorrect").alias("No.of Incorrect Responses"),
        col("assessBlank").alias("Unattempted Questions"),
        col("retakes").alias("No. of Retakes"),
        col("assessEndTime"),
        col("userOrgID").alias("mdoid")
      ).repartition(1)


    // Join using a single column or Seq if multiple columns
    val userOrgHierarchyDataDF = userOrgDF.join(broadcast(orgHierarchyData), Seq("userOrgID"), "left")
    val oldAssessmentData = oldAssessmentDetailsDataframe()
    val fullReportDFOld = oldAssessmentData.join(allCourseProgramDetailsDF, Seq("courseID"), "left").select(
      oldAssessmentData.columns.map(col) ++
        Seq(col("courseName"), col("courseOrgID")): _*)
      .join(userOrgHierarchyDataDF, Seq("userID"), "left")
      .withColumn("Assessment Type", lit("Learning Resource"))
      .withColumn("Total Questions", col("correct_count") + col("incorrect_count") + col("not_answered_count"))
      .withColumn("Assessment Publish Date", lit(null).cast("date"))
      .withColumn("Assessment Duration", lit(null).cast("string"))
      .withColumn("Last Attempted Date", lit(null).cast("date"))
      .withColumn("No. of Retakes", lit(null).cast("integer"))
      .withColumn("assessEndTime", lit(null).cast("bigint"))
      .withColumn("assessChildID", lit(null).cast("string"))
      .withColumn("Pass", when(col("result_percent") >= col("pass_percent"), lit("Yes")).otherwise(lit("No")))
      .withColumn("Tags", concat_ws(", ", col("additionalProperties.tag")))
      .select(
        col("userID").alias("User ID"),
        col("source_id").alias("assessID"),
        col("courseOrgID"),
        col("assessChildID"),
        col("userOrgID"),
        col("fullName").alias("Full Name"),
        col("professionalDetails.designation").alias("Designation"),
        col("personalDetails.primaryEmail").alias("E mail"),
        col("personalDetails.mobile").alias("Phone Number"),
        col("professionalDetails.group").alias("Group"),
        col("Tags"),
        col("ministry_name").alias("Ministry"),
        col("dept_name").alias("Department"),
        col("userOrgName").alias("Organisation"),
        col("source_title").alias("Assessment Name"),
        col("Assessment Type"),
        col("courseOrgID").alias("Assessment/Content Provider"),
        col("Assessment Publish Date"),
        col("courseName").alias("Course Name"),
        col("courseID").alias("Course ID"),
        col("Assessment Duration"),
        col("Last Attempted Date"),
        col("result_percent").alias("Latest Percentage Achieved"),
        col("pass_percent").alias("Cut off Percentage"),
        col("Pass"),
        col("Total Questions"),
        col("incorrect_count").alias("No.of Incorrect Responses"),
        col("not_answered_count").alias("Unattempted Questions"),
        col("`No. of Retakes`"),
        col("assessEndTime"),
        col("userOrgID").alias("mdoid")
      ).repartition(1)

    val fullReportDF = fullReportDFNew.union(fullReportDFOld)
      .withColumn("Report_Last_Generated_On", currentDateTime)
    val reportPath = s"${conf.cbaReportPath}/${today}"
    // generateReport(fullReportDF, s"${reportPath}-full")

    val mdoReportDF = fullReportDF
      .select(
        col("User ID"),
        col("Full Name"),
        col("Designation"),
        col("E mail"),
        col("Phone Number"),
        col("Group"),
        col("Tags"),
        col("Ministry"),
        col("Department"),
        col("Organisation"),
        col("Assessment Name"),
        col("Assessment Type"),
        col("Assessment/Content Provider"),
        col("Assessment Publish Date"),
        col("Course Name"),
        col("Course ID"),
        col("Assessment Duration"),
        col("Last Attempted Date"),
        col("Latest Percentage Achieved"),
        col("Cut off Percentage"),
        col("Pass"),
        col("Total Questions"),
        col("No.of Incorrect Responses"),
        col("Unattempted Questions"),
        col("No. of Retakes"),
        col("mdoid"),
        col("Report_Last_Generated_On")
      ).coalesce(1)
    generateReport(mdoReportDF, reportPath,"mdoid", "UserAssessmentReport")
    // to be removed once new security job is created
    if (conf.reportSyncEnable) {
      syncReports(s"${conf.localReportDir}/${reportPath}", reportPath)
    }

    val warehouseDF = finalDF
      .withColumn("data_last_generated_on", currentDateTime)
      .select(
        col("User ID").alias("user_id"),
        col("Course ID").alias("content_id"),
        col("assessChildID").alias("assessment_id"),
        col("Assessment Name").alias("assessment_name"),
        col("Assessment Type").alias("assessment_type"),
        col("Assessment Duration").alias("assessment_duration"),
        col("Assessment Duration").alias("time_spent_by_the_user"),
        //from_unixtime(col("assessEndTime"), "dd/MM/yyyy").alias("completion_date"),
        date_format(from_unixtime(col("assessEndTime")), "yyyy-MM-dd HH:mm:ss").alias("completion_date"),
        col("Latest Percentage Achieved").alias("score_achieved"),
        col("Total Questions").alias("overall_score"),
        col("Cut off Percentage").alias("cut_off_percentage"),
        col("Total Questions").alias("total_question"),
        col("`No.of Incorrect Responses`").alias("number_of_incorrect_responses"),
        col("`No. of Retakes`").alias("number_of_retakes"),
        col("data_last_generated_on")
      )
    generateReport(warehouseDF.coalesce(1), s"${reportPath}-warehouse")

    Redis.closeRedisConnect()

  }
}
