
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig, Redis}
import org.ekstep.analytics.framework.FrameworkContext

object UserActivityModel extends AbsDashboardModel {

  implicit val className: String = "org.ekstep.analytics.dashboard.report.enrolment.UserActivityModel"

  override def name() = "UserActivityModel"

  /**
   * Master method, does all the work, fetching, processing and dispatching
   *
   * @param timestamp unique timestamp from the start of the processing
   */
  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    val today = getDate()

    //GET ORG DATA
    val (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()
    val orgHierarchyData = orgHierarchyDataframe()
    val userDataDF = userOrgDF
      .join(broadcast(orgHierarchyData), Seq("userOrgID"), "left")
      .withColumn("designation", coalesce(col("professionalDetails.designation"), lit("")))

    // Get course data first
    val allCourseProgramDetailsDF = contentWithOrgDetailsDataFrame(orgDF, Seq("Course", "Program", "Blended Program", "CuratedCollections", "Curated Program"))

    val userEnrolmentDF = userCourseProgramCompletionDataFrame()

    val userRatingDF = userCourseRatingDataframe()

    //use allCourseProgramDetailsDFWithOrgName below instead of allCourseProgramDetailsDF after adding orgname alias above
    val allCourseProgramCompletionWithDetailsDF = allCourseProgramCompletionWithDetailsDataFrame(userEnrolmentDF, allCourseProgramDetailsDF, userDataDF)

    val courseBatchDF = courseBatchDataFrame()
    val relevantBatchInfoDF = allCourseProgramDetailsDF.select("courseID", "category")
      .where(col("category").equalTo("Blended Program"))
      .join(courseBatchDF, Seq("courseID"), "left")
      .select("courseID", "batchID", "courseBatchName", "courseBatchStartDate", "courseBatchEndDate")

    val ciosDataSchema = new StructType().add("content", new StructType()
      .add("name", StringType)
      .add("duration", StringType)
      .add("lastUpdatedOn", StringType)
      .add("contentPartner", new StructType()
        .add("id", StringType)
        .add("contentPartnerName", StringType)))

    val marketPlaceContentsDF = marketPlaceContentDF()
    val parsedDF = marketPlaceContentsDF.withColumn("parsed_data", from_json(col("cios_data"), ciosDataSchema))
    val marketPlaceEnrolmentsDF = marketPlaceEnrolments().withColumnRenamed("courseid", "content_id")
    // Extract the desired fields
    val extractedDF = parsedDF.select(col("content_id"),
      col("parsed_data.content.name").as("courseName"),
      col("parsed_data.content.duration").as("courseDuration"),
      col("parsed_data.content.lastUpdatedOn").as("courseLastPublishedOn"),
      col("parsed_data.content.contentPartner.id").as("courseOrgID"),
      col("parsed_data.content.contentPartner.contentPartnerName").as("courseOrgName"),
      lit("External Content").as("category"),
      lit("LIVE").as("courseStatus"))

    var marketPlaceContentEnrolmentsDF = extractedDF.join(marketPlaceEnrolmentsDF, Seq("content_id"), "inner").durationFormat("courseDuration")
      .withColumn("courseCompletedTimestamp", date_format(col("completedon"), dateTimeFormat))
      .withColumn("courseEnrolledTimestamp", date_format(col("enrolled_date"), dateTimeFormat))
      .withColumn("lastContentAccessTimestamp", lit("Not Available"))
      .withColumn("userRating", lit("Not Available"))
      .withColumn("live_cbp_plan_mandate", lit(false))
      .withColumn("batchID", lit("Not Available"))
      .withColumn("issuedCertificateCount", size(col("issued_certificates")))
      .withColumn("certificate_generated", expr("CASE WHEN issuedCertificateCount > 0 THEN 'Yes' ELSE 'No' END"))
      .withColumn("certificateGeneratedOn", when(col("issued_certificates").isNull, "").otherwise( col("issued_certificates")(size(col("issued_certificates")) - 1).getItem("lastIssuedOn")))
      .withColumn("firstCompletedOn", when(col("issued_certificates").isNull, "").otherwise(when(size(col("issued_certificates")) > 0, col("issued_certificates")(0).getItem("lastIssuedOn")).otherwise("")))
      .withColumn("certificateID", when(col("issued_certificates").isNull, "").otherwise( col("issued_certificates")(size(col("issued_certificates")) - 1).getItem("identifier")))
      .withColumn("Report_Last_Generated_On", currentDateTime)
      .withColumnRenamed("userid", "userID")
      .withColumnRenamed("content_id", "courseID")
      .withColumnRenamed("progress", "courseProgress")
      .withColumnRenamed("status", "dbCompletionStatus")
      .na.fill(0, Seq("courseProgress", "issuedCertificateCount"))
      .na.fill("", Seq("certificateGeneratedOn"))

    val marketPlaceEnrolmentsWithUserDetailsDF = marketPlaceContentEnrolmentsDF.join(userDataDF, Seq("userID"), "left").durationFormat("courseDuration").withColumn("Tag", concat_ws(", ", col("additionalProperties.tag")))
    val allCourseProgramCompletionWithDetailsWithBatchInfoDF = allCourseProgramCompletionWithDetailsDF.join(relevantBatchInfoDF, Seq("courseID", "batchID"), "left")

    val allCourseProgramCompletionWithDetailsDFWithRating = allCourseProgramCompletionWithDetailsWithBatchInfoDF.join(userRatingDF, Seq("courseID", "userID"), "left")

    val df = allCourseProgramCompletionWithDetailsDFWithRating
      .durationFormat("courseDuration")
      .withColumn("completedOn", date_format(col("courseCompletedTimestamp"), dateTimeFormat))
      .withColumn("enrolledOn", date_format(col("courseEnrolledTimestamp"), dateTimeFormat))
      .withColumn("firstCompletedOn", date_format(col("firstCompletedOn"), dateTimeFormat))
      .withColumn("lastContentAccessTimestamp", date_format(col("lastContentAccessTimestamp"), dateTimeFormat))
      .withColumn("courseLastPublishedOn", to_date(col("courseLastPublishedOn"), dateFormat))
      .withColumn("courseBatchStartDate", to_date(col("courseBatchStartDate"), dateFormat))
      .withColumn("courseBatchEndDate", to_date(col("courseBatchEndDate"), dateFormat))
      .withColumn("completionPercentage", round(col("completionPercentage"), 2))
      .withColumn("Tag", concat_ws(", ", col("additionalProperties.tag")))
      .withColumn("Report_Last_Generated_On", currentDateTime)
      .withColumn("Certificate_Generated", expr("CASE WHEN issuedCertificateCount > 0 THEN 'Yes' ELSE 'No' END"))
      .withColumn("ArchivedOn", expr("CASE WHEN courseStatus == 'Retired' THEN lastStatusChangedOn ELSE '' END"))
      .withColumn("ArchivedOn", to_date(col("ArchivedOn"), dateFormat))
      .withColumn("Certificate_ID", col("certificateID"))
      .dropDuplicates("userID", "courseID", "batchID")

    allCourseProgramCompletionWithDetailsDFWithRating.persist(StorageLevel.MEMORY_ONLY)

    // read acbp data and filter the cbp plan based on status
    val acbpDF = acbpDetailsDF().where(col("acbpStatus") === "Live")

    val selectColumns = Seq("userID", "designation", "userOrgID", "acbpID", "assignmentType", "acbpCourseIDList","acbpStatus", "userStatus")
    val acbpAllotmentDF = explodedACBPDetails(acbpDF, userDataDF, selectColumns)

    // replace content list with names of the courses instead of ids
    val acbpAllEnrolmentDF = acbpAllotmentDF
      .withColumn("courseID", explode(col("acbpCourseIDList"))).withColumn("liveCBPlan", lit(true))
      .select(col("userOrgID"),col("courseID"),col("userID"),col("designation"),col("liveCBPlan"))

    val enrolmentWithACBP = df.join(acbpAllEnrolmentDF, Seq("userID", "userOrgID", "courseID"), "left")
      .withColumn("live_cbp_plan_mandate", when(col("liveCBPlan").isNull, false).otherwise(col("liveCBPlan")))

    val marketPlaceWarehouseDF = marketPlaceEnrolmentsWithUserDetailsDF
      .withColumn("certificate_generated_on_str", from_utc_timestamp(to_utc_timestamp(to_timestamp(
        col("certificateGeneratedOn"), dateTimeWithMilliSecFormat), "UTC"), "IST"))
      .withColumn("created_date_str", currentDateTime)
      .withColumn("created_date", col("created_date_str").cast("timestamp"))
      .withColumn("enrolled_on", col("courseEnrolledTimestamp").cast("timestamp"))
      .withColumn("certificate_generated_on", col("certificate_generated_on_str").cast("timestamp"))
      .select(
        col("userID").alias("user_id"),
        col("courseID").alias("type_identifier"),
        col("batchID").alias("batch_id"),
        col("enrolled_on"),
        when(col("dbCompletionStatus").isNull, "not-enrolled")
          .when(col("dbCompletionStatus") === 0, "not-started")
          .when(col("dbCompletionStatus") === 1, "in-progress")
          .otherwise("completed")
          .alias("status"),
        col("certificate_generated_on"),
        col("category").alias("type"),
        col("userOrgID").alias("org_id"),
        col("created_date")
      ).dropDuplicates("user_id", "type_identifier", "batch_id")

    val platformWarehouseDF = enrolmentWithACBP
      .withColumn("certificate_generated_on_str", date_format(from_utc_timestamp(to_utc_timestamp(to_timestamp(
        col("certificateGeneratedOn"), dateTimeWithMilliSecFormat), "UTC"), "IST"), dateTimeFormat))
      .withColumn("created_date_str", currentDateTime)
      .withColumn("enrolled_on", col("enrolledOn").cast("timestamp"))
      .withColumn("created_date", col("created_date_str").cast("timestamp"))
      .withColumn("certificate_generated_on", col("certificate_generated_on_str").cast("timestamp"))
      .select(
        col("userID").alias("user_id"),
        col("courseID").alias("type_identifier"),
        col("batchID").alias("batch_id"),
        col("enrolled_on"),
        when(col("dbCompletionStatus").isNull, "not-enrolled")
          .when(col("dbCompletionStatus") === 0, "not-started")
          .when(col("dbCompletionStatus") === 1, "in-progress")
          .otherwise("completed")
          .alias("status"),
        col("certificate_generated_on"),
        col("category").alias("type"),
        col("userOrgID").alias("org_id"),
        col("created_date")
      ).dropDuplicates("user_id", "type_identifier", "batch_id")

    val warehouseDF = platformWarehouseDF.union(marketPlaceWarehouseDF)

    show(warehouseDF, "warehouse DF")

    val eventEnrolments = cache.load("eventEnrolmentDetails")


    val eventEnrolmentsDF = eventEnrolments.join(broadcast(userDataDF), eventEnrolments("user_id") === userDataDF("userID"), "left")
      .withColumn("created_date_str", currentDateTime)
      .withColumn("created_date", col("created_date_str").cast("timestamp"))
      .withColumn("created_date", col("completed_on_datetime").cast("timestamp"))
      .withColumn("type", lit("Event"))
      .withColumn("batch_id", lit(""))
      .withColumn("enrolled_on", col("enrolled_on_datetime").cast("timestamp"))
      .select(
        col("user_id"),
        col("event_id").alias("type_identifier"),
        col("batch_id"),
        col("enrolled_on"),
        col("status"),
        col("completed_on_datetime").alias("certificate_generated_on"),
        col("type"),
        col("userOrgID").alias("org_id"),
        col("created_date")
      ).dropDuplicates("user_id", "type_identifier", "batch_id")

    show(eventEnrolmentsDF, "eventEnrolmentsDF")

    val userActivityDF = warehouseDF.union(eventEnrolmentsDF)
      .withColumn("created_date_ts", col("created_date").cast("timestamp"))
      .withColumn("certificate_generated_on_ts", col("certificate_generated_on").cast("timestamp"))
      .withColumn("enrolled_on_ts", col("enrolled_on").cast("timestamp"))
      .select(
        col("user_id"),
        col("type_identifier"),
        col("batch_id"),
        col("enrolled_on_ts").alias("enrolled_on"),
        col("status"),
        col("certificate_generated_on_ts").alias("certificate_generated_on"),
        col("type"),
        col("org_id"),
        col("created_date_ts").alias("created_date")
      ).dropDuplicates("user_id", "type_identifier", "batch_id")
    show(userActivityDF, "userActivityDF")

    val dwPostgresUrl = s"jdbc:postgresql://${conf.dwPostgresHost}/${conf.dwPostgresSchema}"
    truncateWarehouseTable("user_activity")
    saveDataframeToPostgresTable_With_Append(userActivityDF, dwPostgresUrl, "user_activity", conf.dwPostgresUsername, conf.dwPostgresCredential)

    allCourseProgramCompletionWithDetailsDFWithRating.unpersist()

    Redis.closeRedisConnect()

  }
}