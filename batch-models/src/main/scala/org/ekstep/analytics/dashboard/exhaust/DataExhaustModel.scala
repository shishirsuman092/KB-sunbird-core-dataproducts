package org.ekstep.analytics.dashboard.exhaust

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.apache.spark.sql.types._
import scala.util.{Try, Success, Failure}
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.concurrent.duration._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig}
import org.ekstep.analytics.framework._
import scala.concurrent.duration._

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
    try {

    import spark.implicits._
    val enrolmentDF = cassandraTableAsDataFrame(conf.cassandraCourseKeyspace, conf.cassandraUserEnrolmentsTable)
    cache.write(enrolmentDF, "enrolment");
    pqCache.write(enrolmentDF, "enrolment")

      enrolmentDF.unpersist()

    val batchDF = cassandraTableAsDataFrame(conf.cassandraCourseKeyspace, conf.cassandraCourseBatchTable)
    cache.write(batchDF, "batch")
    pqCache.write(batchDF, "batch")
    batchDF.unpersist()

    val kcmV6Hierarchy = cassandraTableAsDataFrame(conf.cassandraHierarchyStoreKeyspace, conf.cassandraFrameworkHierarchyTable)
      .filter(col("identifier") === "kcmfinal_fw")
    cache.write(kcmV6Hierarchy, "kcmV6")
    pqCache.write(kcmV6Hierarchy, "kcmV6")
    kcmV6Hierarchy.unpersist()

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
    pqCache.write(finalAssessmentDF, "userAssessment")
    userAssessmentDF.unpersist()

    val hierarchyDF = cassandraTableAsDataFrame(conf.cassandraHierarchyStoreKeyspace, conf.cassandraContentHierarchyTable)
    cache.write(hierarchyDF, "hierarchy")
    pqCache.write(hierarchyDF, "hierarchy")
    hierarchyDF.unpersist()

    val ratingSummaryDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraRatingSummaryTable)
    cache.write(ratingSummaryDF, "ratingSummary")
    pqCache.write(ratingSummaryDF, "ratingSummary")
    ratingSummaryDF.unpersist()

    val acbpDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraAcbpTable)
    cache.write(acbpDF, "acbp")
    pqCache.write(acbpDF, "acbp")
    acbpDF.unpersist()

    val ratingDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraRatingsTable)
    cache.write(ratingDF, "rating")
    pqCache.write(ratingDF, "rating")
    ratingDF.unpersist()

    val roleDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraUserRolesTable)
    cache.write(roleDF, "role")
    pqCache.write(roleDF, "role")
    roleDF.unpersist()

    // ES content
    val primaryCategories = Seq("Course","Program","Blended Program","Curated Program","Standalone Assessment","CuratedCollections","Moderated Course")
    val shouldClause = primaryCategories.map(pc => s"""{"match":{"primaryCategory.raw":"${pc}"}}""").mkString(",")
    val fields = Seq("identifier", "name", "primaryCategory", "status", "reviewStatus", "channel", "duration", "leafNodesCount", "lastPublishedOn", "lastStatusChangedOn", "createdFor", "competencies_v6", "programDirectorName","language","courseCategory")
    val arrayFields = Seq("createdFor","language")
    val fieldsClause = fields.map(f => s""""${f}"""").mkString(",")
    val query = s"""{"_source":[${fieldsClause}],"query":{"bool":{"should":[${shouldClause}]}}}"""
    val esContentDF = elasticSearchDataFrame(conf.sparkElasticsearchConnectionHost, "compositesearch", query, fields, arrayFields)
    cache.write(esContentDF, "esContent")
    pqCache.write(esContentDF, "esContent")

    val orgDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraOrgTable)
    cache.write(orgDF, "org")
    pqCache.write(orgDF, "org")

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
    val orgDfWithSborgid = orgDfWithOrgType
      .join(
        orgPostgresDF.select(col("sborgid").alias("ministry_id_sborgid"), col("mapid").alias("l1mapid_lookup")),
        col("l1mapid") === col("l1mapid_lookup"),
        "left").join(
        orgPostgresDF.select(col("sborgid").alias("department_id_sborgid"), col("mapid").alias("l2mapid_lookup")),
        col("l2mapid") === col("l2mapid_lookup"),
        "left").drop("l1mapid_lookup", "l2mapid_lookup")

    val orgHierarchyDF = orgDfWithSborgid
      .select(
        col("sborgid").alias("mdo_id"),
        col("cassOrgName").alias("mdo_name"),
        col("l1orgname").alias("ministry"),
        col("ministry_id_sborgid").alias("ministry_id"),
        col("l2orgname").alias("department"),
        col("department_id_sborgid").alias("department_id"),
        col("orgCreatedDate").alias("mdo_created_on"),
        col("orgType")
      )
      .withColumn("data_last_generated_on", currentDateTime)
      .distinct()
      .drop("orgType")
      .dropDuplicates(Seq("mdo_id"))
      .repartition(16)
    cache.write(orgHierarchyDF, "orgHierarchy")
    pqCache.write(orgHierarchyDF, "orgHierarchy")
    cache.write(orgPostgresDF, "orgCompleteHierarchy")
    pqCache.write(orgPostgresDF, "orgCompleteHierarchy")
    orgDF.unpersist()

    val ES_HOST = conf.sparkElasticsearchAuditConnectionHost
    val ES_INDEX = "kp_audit"
    val batchSize = 100
    val timeoutSeconds = 30

    val log_record_schema = StructType(Seq(
        StructField("properties", StructType(Seq(
          StructField("lastPublishedOn", StructType(Seq(
            StructField("ov", StringType),
            StructField("nv", StringType)
          ))),
          StructField("status", StructType(Seq(
            StructField("ov", StringType),
            StructField("nv", StringType)
          )))
        )))
      ))

    val (_, _, allCourseProgramDetailsDF, _) = contentDataFrames(
        orgDF,
        Seq("Course", "Program", "Blended Program", "Curated Program", "Standalone Assessment", "CuratedCollections", "Moderated Course")
      )

    val liveCourseIds = allCourseProgramDetailsDF
        .filter(col("courseStatus") === "Live")
        .select("courseID")
        .distinct()
        .collect()
        .map(_.getAs[String]("courseID"))

    println(s"Total live content IDs to process: ${liveCourseIds.length}")

    val allLogs = liveCourseIds.grouped(batchSize).zipWithIndex.flatMap { case (batch, i) =>
        println(s"Processing batch ${i + 1} / ${(liveCourseIds.length + batchSize - 1) / batchSize}")
        fetchLivePublishLogsForBatch(batch, ES_HOST, ES_INDEX, log_record_schema, timeoutSeconds)
      }.toSeq

    println(s"Total content publish records fetched: ${allLogs.length}")
    val contentPublishedOnDF = spark.createDataFrame(allLogs).toDF("content_id", "published_on")

    println("Writing content publish logs to cache...")
    cache.write(contentPublishedOnDF, "contentPublishedOn")
    pqCache.write(contentPublishedOnDF, "contentPublishedOn")
    println("Writing complete.")

    val marketPlaceContentDF = postgresTableAsDataFrame(appPostgresUrl, "cios_content_entity", conf.appPostgresUsername, conf.appPostgresCredential)
    cache.write(marketPlaceContentDF, "externalContent")
    pqCache.write(marketPlaceContentDF, "externalContent")
    marketPlaceContentDF.unpersist()

    val marketPlaceEnrolmentsDF = cassandraTableAsDataFrame("sunbird_courses", "user_external_enrolments")
    cache.write(marketPlaceEnrolmentsDF, "externalCourseEnrolments")
    pqCache.write(marketPlaceEnrolmentsDF, "externalCourseEnrolments")
    marketPlaceEnrolmentsDF.unpersist()

    val userDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraUserTable)
    cache.write(userDF, "user")
    pqCache.write(userDF, "user")
    userDF.unpersist()

    val learnerLeaderboardDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraLearnerLeaderBoardTable)
    cache.write(learnerLeaderboardDF, "learnerLeaderBoard")
    pqCache.write(learnerLeaderboardDF, "learnerLeaderBoard")
    learnerLeaderboardDF.unpersist()

    val userKarmaPointsDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraKarmaPointsTable)
    cache.write(userKarmaPointsDF, "userKarmaPoints")
    pqCache.write(userKarmaPointsDF, "userKarmaPoints")
    userKarmaPointsDF.unpersist()

    val userKarmaPointsSummaryDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraKarmaPointsSummaryTable)
    cache.write(userKarmaPointsSummaryDF, "userKarmaPointsSummary")
    pqCache.write(userKarmaPointsSummaryDF, "userKarmaPointsSummary")
    userKarmaPointsSummaryDF.unpersist()

    val oldAssessmentDetailsDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraOldAssesmentTable)
    cache.write(oldAssessmentDetailsDF, "oldAssessmentDetails")
    pqCache.write(oldAssessmentDetailsDF, "oldAssessmentDetails")
    oldAssessmentDetailsDF.unpersist()

    val weeklyClapsDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraLearnerStatsTable)
    cache.write(weeklyClapsDF, "weeklyClaps")
    pqCache.write(weeklyClapsDF, "weeklyClaps")
    weeklyClapsDF.unpersist()

    //NLW event data
    val objectType = Seq("Event")
    val shouldClauseRequired = objectType.map(pc => s"""{"match":{"objectType.raw":"${pc}"}}""").mkString(",")
    val fieldsRequired = Seq("identifier", "name", "objectType", "status", "startDate", "startTime", "duration", "registrationLink" ,"createdFor", "recordedLinks", "resourceType")
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
      .filter(col("event_start_datetime") >= conf.nationalLearningWeekStart)
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
        col("registrationLink").alias("video_link"),
        col("resourceType").alias("event_tag")
      ).dropDuplicates("event_id")
      .na.fill(0.0, Seq("duration"))
    cache.write(eventDetailsDF, "eventDetails")
    pqCache.write(eventDetailsDF, "eventDetails")

    val caseExpression = "CASE WHEN ISNULL(status) THEN 'not-enrolled' WHEN status == 0 THEN 'not-started' WHEN status == 1 THEN 'in-progress' ELSE 'completed' END"
    val eventsEnrolmentDF = cassandraTableAsDataFrame(conf.cassandraCourseKeyspace, "user_entity_enrolments")
      .withColumn("certificate_id", when(col("issued_certificates").isNull, "").otherwise( col("issued_certificates")(size(col("issued_certificates")) - 1).getItem("identifier")))
      .withColumn("enrolled_on_datetime", date_format(to_utc_timestamp(col("enrolled_date"), "Asia/Kolkata"), dateTimeFormat))
      .withColumn("completed_on_datetime", date_format(to_utc_timestamp(col("completedon"), "Asia/Kolkata"), dateTimeFormat))
      .withColumn("status", expr(caseExpression))
      .withColumn("progress_details", from_json(col("lrc_progressdetails"), Schema.eventProgressDetailSchema))
      .filter(col("enrolled_on_datetime") >= conf.nationalLearningWeekStart)
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
      .withColumn("duration", when(col("progress_details").isNotNull, col("progress_details.duration")).otherwise(null))
      .withColumn("event_duration_seconds", when(col("progress_details").isNotNull, col("progress_details.max_size")).otherwise(null))
      .drop(col("progress_details"))
    // write to cache
    cache.write(eventsEnrolmentWithDurationDF.coalesce(1), "eventEnrolmentDetails")
    pqCache.write(eventsEnrolmentWithDurationDF.coalesce(1), "eventEnrolmentDetails")
    eventsEnrolmentDF.unpersist()
  } catch {
    case e: Exception =>
      println(s"Error occurred during DataExhaustModel processing: ${e.getMessage}", e)
      System.exit(1)
  }
  }
  def fetchLivePublishLogsForBatch(courseIds: Seq[String], ES_HOST: String, ES_INDEX: String, log_record_schema: StructType, timeoutSeconds: Int)
                                  (implicit spark: SparkSession): Seq[(String, String)] = {

    // Required for .as[(String, String)]
    import spark.implicits._
    val fields = Seq("objectId", "logRecord", "createdOn")
    val batchQuery =
      s"""
         |{
         |  "query": {
         |    "bool": {
         |      "should": [
         |        ${courseIds.map(id => s"""{"term": {"objectId": "$id"}}""").mkString(",")}
         |      ]
         |    }
         |  }
         |}
     """.stripMargin

    val future = Future {
      val df = elasticSearchDataFrame(ES_HOST, ES_INDEX, batchQuery, fields)

      df.withColumn("parsed_log", from_json(col("logRecord"), log_record_schema))
        .filter(col("parsed_log.properties.status.nv") === "Live")
        .select(
          col("objectId").alias("content_id"),
          col("createdOn").cast(StringType).alias("published_on")
        )
        .as[(String, String)]
        .collect().toSeq
    }

    try {
      Await.result(future, timeoutSeconds.seconds)
    } catch {
      case _: TimeoutException =>
        println(s"Timeout while fetching logs for batch with ${courseIds.length} IDs")
        Seq.empty
      case e: Throwable =>
        println(s"Error fetching logs: ${e.getMessage}")
        Seq.empty
    }
  }
}
