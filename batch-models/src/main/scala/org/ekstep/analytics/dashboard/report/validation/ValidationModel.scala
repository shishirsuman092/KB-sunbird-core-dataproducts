package org.ekstep.analytics.dashboard.report.validation

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.ekstep.analytics.dashboard.DashboardUtil.getDate
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig, Redis}
import org.ekstep.analytics.framework._
/**
 * Model for validating rows in warehouse tables
 */
object ValidationModel extends AbsDashboardModel {

  implicit val className: String = "org.ekstep.analytics.dashboard.validation.ValidationModel"
  override def name() = "ValidationModel"

  /**
   * Master method,
   *
   * @param timestamp unique timestamp from the start of the processing
   */
  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {

    val today = getDate()
    val reportPath = s"${conf.localReportDir}/${conf.validationReportPath}/$today"

    // Define CSV paths and their respective validation conditions
    val csvConditions:Map[String, (String, Seq[(String, org.apache.spark.sql.Column)])]  = Map(
      "userReport" -> (
        s"${conf.localReportDir}/${conf.userReportPath}/${today}-warehouse",
        Seq(
          ("UserIDNull", col("user_id").isNull),
          ("UserIDUUIDInvalid", !col("user_id").rlike("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")),
          ("MDOIDNull", col("mdo_id").isNull),
          ("FullNameNoNewLine", col("full_name").contains("\n")),
          ("DesignationNoNewLine", col("designation").contains("\n")),
          ("EmailInvalid", col("email").isNull || !col("email").rlike("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$")),
          ("PhoneNumberInvalid",  !col("phone_number").rlike("^(\\+\\d{1,3}[- ]?)?\\d{10}$")),
          ("TagNoNewLine", col("tag").contains("\n")),
          ("UserRegistrationDateInvalid", !col("user_registration_date").rlike("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$")),
          ("RolesNull", col("roles").isNull),
          ("CreatedByIdUUIDInvalid", col("created_by_id").isNotNull && !col("created_by_id").rlike("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")),
          ("DataLastGeneratedOnInvalid", !col("data_last_generated_on").rlike("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2} (AM|PM)$")),
          ("IsVerifiedKarmayogiInvalid", col("is_verified_karmayogi").isNull || !(col("is_verified_karmayogi") === "true" || col("is_verified_karmayogi") === "false")),
          ("NoOfKarmaPointsInvalid", col("no_of_karma_points").isNull || col("no_of_karma_points").cast("int").isNull || col("no_of_karma_points") === ""),
          ("WeeklyClapsDayBeforeYesterdayInvalid", col("weekly_claps_day_before_yesterday").isNull || col("weekly_claps_day_before_yesterday").cast("int").isNull || col("weekly_claps_day_before_yesterday") === "")
        )
      ),
      "userEnrolmentReport" -> (
        s"${conf.localReportDir}/${conf.userEnrolmentReportPath}/${today}-warehouse",
        Seq(
          ("UserIDNull", col("user_id").isNull),
          ("UserIDUUIDInvalid", !col("user_id").rlike("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")),
          ("BatchIDNull", col("batch_id").isNull),
          ("ContentIDNull", col("content_id").isNull),
          ("ContentProgressPercentageInvalid", col("content_progress_percentage").cast("int").isNull || col("content_progress_percentage").cast("int") < 0 || col("content_progress_percentage").cast("int") > 100),
          ("CertificateGeneratedInvalid", col("certificate_generated").isNotNull && !(col("certificate_generated") === "Yes" || col("certificate_generated") === "No")),
          ("FirstCertificateGeneratedOnInvalid", !col("first_certificate_generated_on").rlike("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$")),
          ("LastCertificateGeneratedOnInvalid", !col("last_certificate_generated_on").rlike("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$")),
          ("UserRatingInvalid", col("user_rating").isNotNull && (col("user_rating").cast("int").isNull || col("user_rating").cast("int") < 0 || col("user_rating").cast("int") > 5)),
          ("ResourceCountConsumedInvalid", col("resource_count_consumed").cast("int").isNull || col("resource_count_consumed").cast("int") < 0),
          ("EnrolledOnInvalid", col("enrolled_on").isNull || !col("enrolled_on").rlike("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$")),
          ("UserConsumptionStatusInvalid", !col("user_consumption_status").isin("completed", "in-progress", "not-enrolled", "not-started")),
          ("DataLastGeneratedOnInvalid", !col("data_last_generated_on").rlike("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2} (AM|PM)$")),
          ("LiveCBPPlanMandateInvalid", col("live_cbp_plan_mandate").isNotNull && !(col("live_cbp_plan_mandate") === "true" || col("live_cbp_plan_mandate") === "false")),
          ("FirstCompletedOnInvalid", col("first_completed_on").isNotNull && !col("first_completed_on").rlike("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$")),
          ("LastAccessedOnInvalid", col("content_last_accessed_on").isNull || !col("content_last_accessed_on").rlike("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$"))
        )
      ),
      "assessmentDetail" -> (
        s"${conf.localReportDir}/${conf.cbaReportPath}/${today}-warehouse",
        Seq(
          ("UserIDNull", col("user_id").isNull),
          ("UserIDUUIDInvalid", !col("user_id").rlike("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")),
          ("ContentIDNull", col("content_id").isNull),
          ("AssessmentIDNull", col("assessment_id").isNull),
          ("AssessmentNameNull", col("assessment_name").isNull),
          ("AssessmentTypeNull", col("assessment_type").isNull),
          ("AssessmentDurationInvalid", !col("assessment_duration").rlike("^\\d{2}:\\d{2}:\\d{2}$")),
          ("TimeSpentByTheUserInvalid", !col("time_spent_by_the_user").rlike("^\\d{1,2}:\\d{2}:\\d{2}$")),
          ("CompletionDateInvalid", !col("completion_date").rlike("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$")),
          ("TotalQuestionInvalid", col("total_question") <= 0)
        )
      ),
      "bpEnrolments" -> (
        s"${conf.localReportDir}/${conf.blendedReportPath}/${today}-warehouse",
        Seq(
          ("UserIDNull", col("user_id").isNull),
          ("UserIDUUIDInvalid", !col("user_id").rlike("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")),
          ("ContentIDNull", col("content_id").isNull),
          ("BatchIDNull", col("batch_id").isNull),
          ("ComponentIDNull", col("component_id").isNull),
          ("LastAccessedOnInvalid", col("last_accessed_on").isNotNull && !col("last_accessed_on").rlike("^\\d{4}-\\d{2}-\\d{2}$")),
          ("OfflineSessionDateInvalid", !col("offline_session_date").rlike("^\\d{4}-\\d{2}-\\d{2}$")),
          ("OfflineSessionStartTimeInvalid", col("offline_session_start_time").isNotNull && !col("offline_session_start_time").rlike("^\\d{1,2}:\\d{2}$")),
          ("OfflineSessionEndTimeInvalid", col("offline_session_end_time").isNotNull && !col("offline_session_end_time").rlike("^\\d{1,2}:\\d{2}$"))
        )
      ),
      "cbPlan" -> (
        s"${conf.localReportDir}/${conf.acbpReportPath}/${today}-warehouse",
        Seq(
          ("CBPlanIDNull", col("cb_plan_id").isNull),
          ("OrgIDNull", col("org_id").isNull),
          ("CreatedByNull", col("created_by").isNull),
          ("CreatedByUUIDInvalid", col("created_by").isNotNull && !col("created_by").rlike("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")),
          ("PlanNameNull", col("plan_name").isNull),
          ("AllotmentTypeNull", col("allotment_type").isNull),
          ("AllotmentToNull", col("allotment_to").isNull),
          ("ContentIDNull", col("content_id").isNull),
          ("AllocatedOnInvalid", !col("allocated_on").rlike("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$")),
          ("DueByInvalid", !col("due_by").rlike("^\\d{4}-\\d{2}-\\d{2}$")),
          ("StatusInvalid", !col("status").isin("Live", "RETIRE", "DRAFT"))
        )
      ),
      "content" -> (
        s"${conf.localReportDir}/${conf.courseReportPath}/${today}-warehouse",
        Seq(
          ("ContentIDNull", col("content_id").isNull),
          ("ContentProviderIDNull", col("content_provider_id").isNull),
          ("ContentProviderNameNull", col("content_provider_name").isNull),
          ("ContentNameNull", col("content_name").isNull),
          //            ("ContentTypeNull", col("content_type").isNull),
          ("ContentDurationInvalid", !col("content_duration").rlike("^\\d{1,2}:\\d{2}:\\d{2}$")),
          ("LastPublishedOnInvalid", !col("last_published_on").rlike("^\\d{4}-\\d{2}-\\d{2}$")),
          ("ContentRetiredOnInvalid", col("content_status") === "Retired" && (col("content_retired_on").isNull || !col("content_retired_on").rlike("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$"))),
          ("ContentStatusInvalid", !col("content_status").isin("Review", "Draft", "Retired", "Live")),
          ("ResourceCountInvalid", col("resource_count") <= 0),
          ("ContentSubstatusInvalid", !col("content_substatus").isin("SentToPublish","Reviewed","InReview"))
        )
      ),
      "contentResource" -> (
        s"${conf.localReportDir}/${conf.courseReportPath}/${today}-resource-warehouse",
        Seq(
          ("ResourceIDNull", col("resource_id").isNull),
          ("ResourceNameNull", col("resource_name").isNull),
          ("ResourceTypeNull", col("resource_type").isNull),
          ("ContentIDNull", col("content_id").isNull)
        )
      ),
      "orgHierarchy" -> (
        s"${conf.localReportDir}/${conf.orgHierarchyReportPath}/${today}-warehouse",
        Seq(
          ("MDOIDNull", col("mdo_id").isNull),
          ("MDONameNull", col("mdo_name").isNull),
          ("IsContentProviderInvalid", !col("is_content_provider").isin("Y", "N")),
          ("DataLastGeneratedOnInvalid", !col("data_last_generated_on").rlike("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2} (AM|PM)$")),
          ("MDOCreatedOnNull", col("mdo_created_on").isNull)
        )
      ),
      "kcmContentMapping" -> (
        s"${conf.localReportDir}/${conf.kcmReportPath}/${today}/ContentCompetencyMapping-warehouse",
        Seq(
          ("CourseIDNull", col("course_id").isNull)
        )
      )
    )
    csvConditions.foreach { case (csvName, (csvPath, conditions)) =>
      val df = spark.read.option("header", "true").csv(csvPath)
      val totalRows = df.count()

      // Calculate counts for each condition
      val validationCounts = conditions.map { case (conditionName, condition) =>
        conditionName -> df.filter(condition).count()
      }

      val totalRowsRow = ("TotalRows", totalRows)

      // Create a DataFrame for results
      val resultsDF = spark.createDataFrame((validationCounts :+ totalRowsRow).map {
        case (conditionName, count) => (conditionName, count)
      }).toDF("Condition", "Count")

      println(csvPath)
      resultsDF.show()

      // Save results to CSV
      resultsDF.coalesce(1).write.mode(SaveMode.Overwrite).format("csv").option("header", "true").save(s"$reportPath/$csvName-validation")
    }

    // Additional validation: Certificates issued match
    val userEnrollmentDF = spark.read.option("header", "true").csv(s"${conf.localReportDir}/${conf.userEnrolmentReportPath}/${today}-warehouse")
    val contentDF = spark.read.option("header", "true").csv(s"${conf.localReportDir}/${conf.courseReportPath}/${today}-warehouse")

    // Aggregate to get total certificates issued per content
    val certificatesPerContentDF = userEnrollmentDF
      .filter(col("certificate_generated") === "true")
      .groupBy("content_id")
      .count()
      .withColumnRenamed("count", "certificates_issued")

    // Join with content DataFrame
    val mismatchDF = certificatesPerContentDF.join(contentDF, Seq("content_id"))
      .filter(col("certificates_issued") =!= col("total_certificates_issued"))

    val mismatchCount = mismatchDF.count()
    val mismatchResultsDF = spark.createDataFrame(Seq(("CertificatesIssuedMismatch", mismatchCount))).toDF("Condition", "Count")

    mismatchResultsDF.show()
    mismatchResultsDF.coalesce(1).write.mode(SaveMode.Overwrite).format("csv").option("header", "true").save(s"$reportPath/CertificatesIssuedMismatch-validation")

    // Close Redis connection
    Redis.closeRedisConnect()
  }
}