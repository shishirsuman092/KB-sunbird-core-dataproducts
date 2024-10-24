package org.ekstep.analytics.dashboard.exhaust

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil.Schema
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig}
import org.ekstep.analytics.framework._

/**
 * Model for processing dashboard data
 */
object DataExhaustModel extends AbsDashboardModel {

  implicit val className: String = "org.ekstep.analytics.dashboard.exhaust.DataExhaustModel"
  override def name() = "DataExhaustModel"

  /**
   * Master method, does all the work, fetching, processing and dispatching
   *
   * @param timestamp unique timestamp from the start of the processing
   */
  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    val enrolmentDF = cassandraTableAsDataFrame(conf.cassandraCourseKeyspace, conf.cassandraUserEnrolmentsTable)
    cache.write(enrolmentDF, "enrolment")
    enrolmentDF.unpersist()

    val batchDF = cassandraTableAsDataFrame(conf.cassandraCourseKeyspace, conf.cassandraCourseBatchTable)
    cache.write(batchDF, "batch")
    batchDF.unpersist()

    val userAssessmentDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraUserAssessmentTable)
      .select(
        col("assessmentid").alias("assessChildID"),
        col("starttime").alias("assessStartTime"),
        col("endtime").alias("assessEndTime"),
        col("status").alias("assessUserStatus"),
        col("userid").alias("userID"),
        col("assessmentreadresponse"),
        col("submitassessmentresponse"),
        col("submitassessmentrequest")
      )
      .na.fill("{}", Seq("submitassessmentresponse", "submitassessmentrequest"))
      .withColumn("readResponse", from_json(col("assessmentreadresponse"), Schema.assessmentReadResponseSchema))
      .withColumn("submitRequest", from_json(col("submitassessmentrequest"), Schema.submitAssessmentRequestSchema))
      .withColumn("submitResponse", from_json(col("submitassessmentresponse"), Schema.submitAssessmentResponseSchema))
      .withColumn("assessStartTimestamp", col("assessStartTime"))
      .withColumn("assessEndTimestamp", col("assessEndTime"))

    val assessWithSchema = userAssessmentDF.select(
      col("assessChildID"),
      col("assessStartTimestamp"),
      col("assessEndTimestamp"),
      col("assessUserStatus"),
      col("userID"),

      col("readResponse.totalQuestions").alias("assessTotalQuestions"),
      col("readResponse.maxQuestions").alias("assessMaxQuestions"),
      col("readResponse.expectedDuration").alias("assessExpectedDuration"),
      col("readResponse.version").alias("assessVersion"),
      col("readResponse.maxAssessmentRetakeAttempts").alias("assessMaxRetakeAttempts"),
      col("readResponse.status").alias("assessReadStatus"),
      col("readResponse.primaryCategory").alias("assessPrimaryCategory"),

      col("submitRequest.batchId").alias("assessBatchID"),
      col("submitRequest.courseId").alias("courseID"),
      col("submitRequest.isAssessment").cast(IntegerType).alias("assessIsAssessment"),
      col("submitRequest.timeLimit").alias("assessTimeLimit"),

      col("submitResponse.result").alias("assessResult"),
      col("submitResponse.total").alias("assessTotal"),
      col("submitResponse.blank").alias("assessBlank"),
      col("submitResponse.correct").alias("assessCorrect"),
      col("submitResponse.incorrect").alias("assessIncorrect"),
      col("submitResponse.pass").cast(IntegerType).alias("assessPass"),
      col("submitResponse.overallResult").alias("assessOverallResult"),
      col("submitResponse.passPercentage").alias("assessPassPercentage")
    )

    val finalAssessmentDF = assessWithSchema.select(
      col("assessChildID"),col("assessUserStatus"),col("userID"),col("assessMaxQuestions"),col("assessExpectedDuration"),col("assessPrimaryCategory"),
      col("assessBlank"),col("assessCorrect"),col("assessIncorrect"),
      col("assessPass"),col("assessOverallResult"),col("assessPassPercentage"), col("courseID"),
      col("assessTotalQuestions"), col("assessVersion"), col("assessMaxRetakeAttempts"), col("assessReadStatus"), col("assessBatchID"), col("assessIsAssessment"), col("assessTimeLimit"),
      col("assessResult"), col("assessTotal"),col("assessStartTimestamp"),
      col("assessEndTimestamp")
    )
    cache.write(finalAssessmentDF, "userAssessment")
    userAssessmentDF.unpersist()

    val hierarchyDF = cassandraTableAsDataFrame(conf.cassandraHierarchyStoreKeyspace, conf.cassandraContentHierarchyTable)
    cache.write(hierarchyDF, "hierarchy")
    hierarchyDF.unpersist()

    val ratingSummaryDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraRatingSummaryTable)
    cache.write(ratingSummaryDF, "ratingSummary")
    ratingSummaryDF.unpersist()

    val acbpDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraAcbpTable)
    cache.write(acbpDF, "acbp")
    acbpDF.unpersist()

    val ratingDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraRatingsTable)
    cache.write(ratingDF, "rating")
    ratingDF.unpersist()

    val roleDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraUserRolesTable)
    cache.write(roleDF, "role")
    roleDF.unpersist()

    // ES content
    val primaryCategories = Seq("Course","Program","Blended Program","Curated Program","Standalone Assessment","CuratedCollections","Moderated Course")
    val shouldClause = primaryCategories.map(pc => s"""{"match":{"primaryCategory.raw":"${pc}"}}""").mkString(",")
    val fields = Seq("identifier", "name", "primaryCategory", "status", "reviewStatus", "channel", "duration", "leafNodesCount", "lastPublishedOn", "lastStatusChangedOn", "createdFor", "competencies_v5", "programDirectorName","language")
    val arrayFields = Seq("createdFor","language")
    val fieldsClause = fields.map(f => s""""${f}"""").mkString(",")
    val query = s"""{"_source":[${fieldsClause}],"query":{"bool":{"should":[${shouldClause}]}}}"""
    val esContentDF = elasticSearchDataFrame(conf.sparkElasticsearchConnectionHost, "compositesearch", query, fields, arrayFields)
    cache.write(esContentDF, "esContent")

    val orgDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraOrgTable)
    cache.write(orgDF, "org")

    // org hierarchy
    val appPostgresUrl = s"jdbc:postgresql://${conf.appPostgresHost}/${conf.appPostgresSchema}"
    val orgPostgresDF = postgresTableAsDataFrame(appPostgresUrl, conf.appOrgHierarchyTable, conf.appPostgresUsername, conf.appPostgresCredential)
    val orgCassandraDF = orgDF
      .withColumn("createddate", to_timestamp(col("createddate"), "yyyy-MM-dd HH:mm:ss:SSSZ"))
      .select(
        col("id").alias("sborgid"),
        col("organisationtype").alias("orgType"),
        col("orgname").alias("cassOrgName"),
        col("createddate").alias("orgCreatedDate")
      )
    val orgDfWithOrgType = orgCassandraDF.join(orgPostgresDF, Seq("sborgid"), "left")
    val orgHierarchyDF = orgDfWithOrgType
      .select(
        col("sborgid").alias("mdo_id"),
        col("cassOrgName").alias("mdo_name"),
        col("l1orgname").alias("ministry"),
        col("l2orgname").alias("department"),
        col("orgCreatedDate").alias("mdo_created_on"),
        col("orgType")
      )
      .withColumn("is_content_provider",
        when(col("orgType").cast("int") === 128 || col("orgType").cast("int") === 128, lit("Y")).otherwise(lit("N")))
      .withColumn("organization", when(col("ministry").isNotNull && col("department").isNotNull, col("mdo_name")).otherwise(null))
      .withColumn("data_last_generated_on", currentDateTime)
      .distinct()
      .drop("orgType")
      .dropDuplicates(Seq("mdo_id"))
      .repartition(16)
    cache.write(orgHierarchyDF, "orgHierarchy")
    cache.write(orgPostgresDF, "orgCompleteHierarchy")
    orgDF.unpersist()

    val marketPlaceContentDF = postgresTableAsDataFrame(appPostgresUrl, "cios_content_entity", conf.appPostgresUsername, conf.appPostgresCredential)
    cache.write(marketPlaceContentDF, "externalContent")
    marketPlaceContentDF.unpersist()

    val marketPlaceEnrolmentsDF = cassandraTableAsDataFrame("sunbird_courses", "user_external_enrolments")
    cache.write(marketPlaceEnrolmentsDF, "externalCourseEnrolments")
    marketPlaceEnrolmentsDF.unpersist()

    val userDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraUserTable)
    cache.write(userDF, "user")
    userDF.unpersist()

    val learnerLeaderboardDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraLearnerLeaderBoardTable)
    cache.write(learnerLeaderboardDF, "learnerLeaderBoard")
    learnerLeaderboardDF.unpersist()

    val userKarmaPointsDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraKarmaPointsTable)
    cache.write(userKarmaPointsDF, "userKarmaPoints")
    userKarmaPointsDF.unpersist()

    val userKarmaPointsSummaryDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraKarmaPointsSummaryTable)
    cache.write(userKarmaPointsSummaryDF, "userKarmaPointsSummary")
    userKarmaPointsSummaryDF.unpersist()

    val oldAssessmentDetailsDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraOldAssesmentTable)
    cache.write(oldAssessmentDetailsDF, "oldAssessmentDetails")
    oldAssessmentDetailsDF.unpersist()

    //NLW event data
    val objectType = Seq("Event")
    val shouldClauseRequired = objectType.map(pc => s"""{"match":{"objectType.raw":"${pc}"}}""").mkString(",")
    val fieldsRequired = Seq("identifier", "name", "objectType", "status", "startDate", "startTime", "duration", "registrationLink" ,"createdFor", "recordedLinks")
    val arrayFieldsRequired = Seq("createdFor","recordedLinks")
    val fieldsClauseRequired = fieldsRequired.map(f => s""""${f}"""").mkString(",")
    val eventQuery = s"""{"_source":[${fieldsClauseRequired}],"query":{"bool":{"should":[${shouldClauseRequired}]}}}"""
    val eventDataDF = elasticSearchDataFrame(conf.sparkElasticsearchConnectionHost, "compositesearch", eventQuery, fieldsRequired, arrayFieldsRequired)
    val eventDetailsDF = eventDataDF
      .withColumn("event_provider_mdo_id", explode_outer(col("createdFor")))
      .withColumn("recording_link", explode_outer(col("recordedLinks")))
      .withColumn("event_start_datetime",concat(substring(col("startDate"), 1,10), lit(" "), substring(col("startTime"), 1, 8)))
      .withColumn("presenters", lit("No presenters available"))
      .withColumn("durationInSecs", col("duration")*60)
      .durationFormat("durationInSecs")
      .select(
        col("identifier").alias("event_id"),
        col("name").alias("event_name"),
        col("event_provider_mdo_id"),
        col("event_start_datetime"),
        col("durationInSecs").alias("duration"),
        col("status").alias("event_status"),
        col("objectType").alias("event_type"),
        col("presenters"),
        col("recording_link"),
        col("registrationLink").alias("video_link")
      ).dropDuplicates("event_id")
      .na.fill(0.0, Seq("duration"))
    cache.write(eventDetailsDF, "eventDetails")

    val caseExpression = "CASE WHEN ISNULL(status) THEN 'not-enrolled' WHEN status == 0 THEN 'not-started' WHEN status == 1 THEN 'in-progress' ELSE 'completed' END"
    val eventsEnrolmentDF = cassandraTableAsDataFrame(conf.cassandraCourseKeyspace, conf.cassandraUserEntityEnrolmentTable)
      .withColumn("certificate_id", when(col("issued_certificates").isNull, "").otherwise( col("issued_certificates")(size(col("issued_certificates")) - 1).getItem("identifier")))
      .withColumn("enrolled_on_datetime", date_format(to_utc_timestamp(col("enrolled_date"), "Asia/Kolkata"), dateTimeFormat))
      .withColumn("completed_on_datetime", date_format(to_utc_timestamp(col("completedon"), "Asia/Kolkata"), dateTimeFormat))
      .withColumn("status", expr(caseExpression))
      .withColumn("progress_details", from_json(col("lrc_progressdetails"), Schema.eventProgressDetailSchema))
      .select(
        col("userid").alias("user_id"),
        col("contentid").alias("event_id"),
        col("status"),
        col("enrolled_on_datetime"),
        col("completed_on_datetime"),
        col("progress_details"),
        col("certificate_id"),
        col("completionpercentage").alias("completion_percentage")
      )
    val eventsEnrolmentWithDurationDF = eventsEnrolmentDF
      .withColumn("event_duration", when(col("progress_details").isNotNull, col("progress_details.max_size")).otherwise(null))
      .durationFormat("event_duration")
      .withColumn("progress_duration", when(col("progress_details").isNotNull, col("progress_details.duration")).otherwise(null))
      .durationFormat("progress_duration")
      .select(
        col("user_id"),
        col("event_id"),
        col("status"),
        col("enrolled_on_datetime"),
        col("completed_on_datetime"),
        col("event_duration"),
        col("progress_duration"),
        col("certificate_id"),
        col("completion_percentage")
      )
    show(eventsEnrolmentWithDurationDF, "eventsEnrolmentWithDurationDF")
    // write to cache
    cache.write(eventsEnrolmentWithDurationDF, "eventEnrolmentDetails")
    eventsEnrolmentDF.unpersist()

  }
}

