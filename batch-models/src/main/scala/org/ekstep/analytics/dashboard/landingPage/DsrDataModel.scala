package org.ekstep.analytics.dashboard.landingPage

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig}
import org.ekstep.analytics.framework._

import java.text.SimpleDateFormat

/**
 * Model for processing dashboard data
 */
object DsrDataModel extends AbsDashboardModel {

  implicit val className: String = "org.ekstep.analytics.dashboard.landingPage.DsrDataModel"

  override def name() = "DsrDataModel"

  /**
   *
   * @param spark
   * @param conf
   * @return
   */
  def getUserDataFrame()(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    val profileDetailsSchema = Schema.makeProfileDetailsSchema(additionalProperties = true, professionalDetails = true)
    var userDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraUserTable)
      .select(
        col("id").alias("userID"),
        col("firstname").alias("firstName"),
        col("lastname").alias("lastName"),
        col("maskedemail").alias("maskedEmail"),
        col("maskedphone").alias("maskedPhone"),
        col("rootorgid").alias("userOrgID"),
        col("status").alias("userStatus"),
        col("profiledetails").alias("userProfileDetails"),
        col("createddate").alias("userCreatedTimestamp"),
        col("updateddate").alias("userUpdatedTimestamp"),
        col("createdby").alias("userCreatedBy")
      )
      .na.fill("", Seq("userOrgID", "firstName", "lastName"))
      .na.fill("{}", Seq("userProfileDetails"))
      .withColumn("profileDetails", from_json(col("userProfileDetails"), profileDetailsSchema))
      .withColumn("personalDetails", col("profileDetails.personalDetails"))
      .withColumn("professionalDetails", explode_outer(col("profileDetails.professionalDetails")))
      .withColumn("userVerified", when(col("profileDetails.verifiedKarmayogi").isNull, false).otherwise(col("profileDetails.verifiedKarmayogi")))
      .withColumn("userMandatoryFieldsExists", col("profileDetails.mandatoryFieldsExists"))
      .withColumn("userProfileImgUrl", col("profileDetails.profileImageUrl"))
      .withColumn("userProfileStatus", col("profileDetails.profileStatus"))
      .withColumn("userPhoneVerified", expr("LOWER(personalDetails.phoneVerified) = 'true'"))
      .withColumn("fullName", concat_ws(" ", col("firstName"), col("lastName")))

    userDF = userDF
      .withColumn("additionalProperties",
        if (userDF.columns.contains("profileDetails.additionalPropertis")) {
          col("profileDetails.additionalPropertis")
        } else {
          col("profileDetails.additionalProperties")
        })
      .drop("profileDetails", "userProfileDetails")

    userDF = timestampStringToLong(userDF, Seq("userCreatedTimestamp", "userUpdatedTimestamp"))
    show(userDF, "userDataFrame")

    userDF
  }

  /**
   *
   * @param spark
   * @param conf
   * @return
   */
  def getUserEnrolment()(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {

    val selectCols = Seq("userID", "courseID", "batchID", "courseProgress", "dbCompletionStatus", "courseCompletedTimestamp",
      "courseEnrolledTimestamp", "lastContentAccessTimestamp", "issuedCertificateCount", "issuedCertificateCountPerContent", "firstCompletedOn", "certificateGeneratedOn", "certificateID")

    var df = cassandraTableAsDataFrame(conf.cassandraCourseKeyspace, conf.cassandraUserEnrolmentsTable)
      .where(expr("active=true"))
      .withColumn("courseCompletedTimestamp", col("completedon"))
      .withColumn("courseEnrolledTimestamp", col("enrolled_date"))
      .withColumn("lastContentAccessTimestamp", col("lastcontentaccesstime"))
      .withColumn("issuedCertificateCount", size(col("issued_certificates")))
      .withColumn("issuedCertificateCountPerContent", when(size(col("issued_certificates")) > 0, lit(1)).otherwise(lit(0)))
      .withColumn("certificateGeneratedOn", when(col("issued_certificates").isNull, "").otherwise(col("issued_certificates")(size(col("issued_certificates")) - 1).getItem("lastIssuedOn")))
      .withColumn("firstCompletedOn", when(col("issued_certificates").isNull, "").otherwise(when(size(col("issued_certificates")) > 0, col("issued_certificates")(0).getItem("lastIssuedOn")).otherwise("")))
      .withColumn("certificateID", when(col("issued_certificates").isNull, "").otherwise(col("issued_certificates")(size(col("issued_certificates")) - 1).getItem("identifier")))
      .withColumnRenamed("userid", "userID")
      .withColumnRenamed("courseid", "courseID")
      .withColumnRenamed("batchid", "batchID")
      .withColumnRenamed("progress", "courseProgress")
      .withColumnRenamed("status", "dbCompletionStatus")
      .withColumnRenamed("contentstatus", "courseContentStatus")
      .na.fill(0, Seq("courseProgress", "issuedCertificateCount"))
      .na.fill("", Seq("certificateGeneratedOn"))
      .select(selectCols.head, selectCols.tail: _*)

    df
  }

  /**
   * Master method, does all the work, fetching, processing and dispatching
   *
   * @param timestamp unique timestamp from the start of the processing
   */
  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {

    val processingTime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").format(timestamp)

    // get all course related data from ES
    // Seq("Course","Program","Blended Program","Curated Program","Moderated Course","Standalone Assessment","CuratedCollections")
    val primaryCategories: Seq[String] = Seq("Course")

    // get all org data from avro
    //val orgDF = orgDataFrame()

    // get all user data from cassandra
    //val userDF = getUserDataFrame() // TODO check if only count is required

    // get all enrolment count
    val enrolmentDF = getUserEnrolment() // todo check if only count is required

    // get all course details
    val allCourseProgramESDF = allCourseProgramFromESDataFrame(primaryCategories)

    // obtain and save user org data
    val (orgDF, userDF, userOrgDF) = getOrgUserDataFrames(getUserDataFrame, orgDataFrame())

    // obtain and save role count data
    val roleDF = roleDataFrame()
    val userOrgRoleDF = userOrgRoleDataFrame(userOrgDF, roleDF)

    // obtain and save org role count data
    val orgRoleCount = orgRoleCountDataFrame(userOrgRoleDF)

    // org user count
    val orgUserCountDF = orgUserCountDataFrame(orgDF, userDF)

    val (hierarchyDF, allCourseProgramDetailsWithCompDF, allCourseProgramDetailsDF,
    allCourseProgramDetailsWithRatingDF) = contentDataFramesForCourses(orgDF, allCourseProgramESDF)

    // get course competency mapping data, dispatch to kafka to be ingested by druid data-source: dashboards-course-competency
    val allCourseProgramCompetencyDF = allCourseProgramCompetencyDataFrame(allCourseProgramDetailsWithCompDF)

    // get course completion data, dispatch to kafka to be ingested by druid data-source: dashboards-user-course-program-progress

    val userCourseProgramCompletionDF = userCourseProgramCompletionDataFrame(datesAsLong = true)
    val allCourseProgramCompletionWithDetailsDF = allCourseProgramCompletionWithDetailsDataFrame(userCourseProgramCompletionDF, allCourseProgramDetailsDF, userOrgDF)

    // enrollment/not-started/started/in-progress/completion count, live and retired courses
    val liveRetiredContentEnrolmentDF = allCourseProgramCompletionWithDetailsDF.where(expr("courseStatus IN ('Live', 'Retired') AND userStatus=1"))
    val liveRetiredCourseEnrolmentDF = allCourseProgramCompletionWithDetailsDF.where(expr("category='Course' AND courseStatus IN ('Live', 'Retired') AND userStatus=1"))
    val liveRetiredCourseProgramEnrolmentDF = allCourseProgramCompletionWithDetailsDF.where(expr("category IN ('Course', 'Program') AND courseStatus IN ('Live', 'Retired') AND userStatus=1"))
    val liveRetiredCourseProgramExcludingModeratedEnrolmentDF = allCourseProgramCompletionWithDetailsDF.where(expr("category IN ('Course', 'Program', 'Blended Program', 'CuratedCollections', 'Standalone Assessment', 'Curated Program') AND courseStatus IN ('Live', 'Retired') AND userStatus=1"))
    val liveRetiredCourseModeratedCourseEnrolmentDF = allCourseProgramCompletionWithDetailsDF.where(expr("category IN ('Course', 'Moderated Course') AND courseStatus IN ('Live', 'Retired') AND userStatus=1"))

    val liveRetiredCourseNotStartedDF = liveRetiredCourseEnrolmentDF.where(expr("dbCompletionStatus=0"))
    val liveRetiredCourseStartedDF = liveRetiredCourseEnrolmentDF.where(expr("dbCompletionStatus IN (1, 2)"))
    // in-progress + completed = started
    val liveRetiredCourseInProgressDF = liveRetiredCourseStartedDF.where(expr("dbCompletionStatus=1"))
    val liveRetiredCourseCompletedDF = liveRetiredCourseStartedDF.where(expr("dbCompletionStatus=2"))
    val liveRetiredCourseEnrolmentsCompletionsDF = liveRetiredCourseStartedDF.where(expr("dbCompletionStatus IN (0, 1, 2)"))
    // course program completed
    val liveRetiredCourseProgramCompletedDF = liveRetiredCourseProgramEnrolmentDF.where(expr("dbCompletionStatus=2"))


    val enrolmentCountDF = liveRetiredCourseEnrolmentDF.agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    val notStartedCountDF = liveRetiredCourseNotStartedDF.agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    val startedCountDF = liveRetiredCourseStartedDF.agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    val inProgressCountDF = liveRetiredCourseInProgressDF.agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    val completedCountDF = liveRetiredCourseCompletedDF.agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    val landingPageCompletedCountDF = liveRetiredCourseProgramCompletedDF.agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))

    // unique user counts
    val enrolmentUniqueUserCount = enrolmentCountDF.select("uniqueUserCount").first().getLong(0)
    val notStartedUniqueUserCount = notStartedCountDF.select("uniqueUserCount").first().getLong(0)
    val startedUniqueUserCount = startedCountDF.select("uniqueUserCount").first().getLong(0)
    val inProgressUniqueUserCount = inProgressCountDF.select("uniqueUserCount").first().getLong(0)
    val completedUniqueUserCount = completedCountDF.select("uniqueUserCount").first().getLong(0)

    var dsrDF = emptySchemaDataFrame(Schema.dsrMetricSchema);

    dsrDF
      .withColumn("central_ministries", coalesce(col("central_ministries").cast("int"), lit(89)))
      .withColumn("state_ut", coalesce(col("state_ut").cast("int"), lit(89)))
      .withColumn("department_organisations_onboarded", coalesce(col("department_organisations_onboarded").cast("int"), lit(89)))
      .withColumn("org_with_mdo_admin_leader_count", coalesce(col("org_with_mdo_admin_leader_count").cast("int"), lit(89)))
      .withColumn("org_with_mdo_admin_count", coalesce(col("org_with_mdo_admin_count").cast("int"), lit(89)))
      .withColumn("org_with_live_course_count", coalesce(col("org_with_live_course_count").cast("int"), lit(89)))
      .withColumn("total_course_publishers", coalesce(col("total_course_publishers").cast("int"), lit(89)))
      .withColumn("total_courses_published", coalesce(col("total_courses_published").cast("int"), lit(89)))
      .withColumn("total_courses_draft", coalesce(col("total_courses_draft").cast("int"), lit(89)))
      .withColumn("total_courses_review", coalesce(col("total_courses_review").cast("int"), lit(89)))
      .withColumn("total_courses_retired", coalesce(col("total_courses_retired").cast("int"), lit(89)))
      .withColumn("total_courses_pending_published", coalesce(col("total_courses_pending_published").cast("int"), lit(89)))
      .withColumn("total_course_duration", coalesce(col("total_course_duration").cast("int"), lit(89)))
      .withColumn("total_course_enrolments", coalesce(col("total_course_enrolments").cast("int"), lit(89)))
      .withColumn("total_course_completions", coalesce(col("total_course_completions").cast("int"), lit(89)))
      .withColumn("total_user_count", coalesce(col("total_user_count").cast("int"), lit(89)))
      .withColumn("new_user_registrations_yesterday", coalesce(col("new_user_registrations_yesterday").cast("int"), lit(89)))
      .withColumn("user_logged_in_yesterday", coalesce(col("user_logged_in_yesterday").cast("int"), lit(89)))
      .withColumn("users_enrolled_in_at_least_one_course", coalesce(col("users_enrolled_in_at_least_one_course").cast("int"), lit(89)))

    show(dsrDF, "DSR DF")

    saveDataframeToPostgresTable_With_Append(dsrDF, conf.dwPostgresHost,"", conf.dwPostgresUsername, conf.dwPostgresCredential)
  }
}

