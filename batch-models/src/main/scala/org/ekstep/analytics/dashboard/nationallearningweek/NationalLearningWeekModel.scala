package org.ekstep.analytics.dashboard.nationallearningweek

import org.apache.spark.SparkContext
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, IntegerType, LongType, BooleanType, StructType}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig, Redis}
import org.ekstep.analytics.framework.FrameworkContext
import org.apache.spark.sql.expressions.UserDefinedFunction
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import java.time.{LocalDate, ZoneOffset}

object NationalLearningWeekModel extends AbsDashboardModel {

  implicit val className: String = "org.ekstep.analytics.dashboard.nationallearningweek.NationalLearningWeekModel"

  override def name() = "NationalLearningWeekModel"

  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    try {
      def timeToHoursUDF: UserDefinedFunction = udf((timeStr: String) => {
        if (timeStr != null && timeStr.matches("\\d{1,2}:\\d{2}:\\d{2}")) {
          val parts = timeStr.split(":").map(_.toDouble)
          parts(0) + parts(1) / 60 + parts(2) / 3600
        } else {
          0.0
        }
      })
      val appPostgresUrl =  s"jdbc:postgresql://${conf.appPostgresHost}/${conf.appPostgresSchema}"
      var nlw_mdo_id = "01358339603629670470"
      val stateLearningWeekStartString = conf.stateLearningWeekStart
      val stateLearningWeekEndString = conf.stateLearningWeekEnd
      val zoneOffset = ZoneOffset.ofHoursMinutes(5, 30)

      val currentDate = LocalDate.now()
      val previousDayStart = currentDate.minusDays(1).atStartOfDay().atOffset(zoneOffset)
      val previousDayEnd = currentDate.atStartOfDay().minusSeconds(1).atOffset(zoneOffset)
      val eventsDateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
      val previousStart = previousDayStart.format(eventsDateTimeFormatter)
      val previousEnd = previousDayEnd.format(eventsDateTimeFormatter)

      val contentEnrolmentsDF = warehouseCache.load(conf.dwEnrollmentsTable)
      val contentDetailsDF = warehouseCache.load(conf.dwCourseTable)
      val eventsEnrolmentsDF = warehouseCache.load("event_enrolment_details")
      val eventDetailsDF = warehouseCache.load("event_details")
      val userDetailsDF = warehouseCache.load(conf.dwUserTable)
      val contentDF = warehouseCache.load(conf.dwCourseTable)
      val eventsDF = cache.load("eventDetails")
      val orgHierarchyDF = cache.load("orgHierarchy")

      val eventCertificatesGeneratedSLWYdayDF = eventsEnrolmentsDF
        .filter(col("completed_on_datetime") >= previousStart && col("completed_on_datetime") <= previousEnd)
        .filter(col("certificate_id").isNotNull)
        .join(userDetailsDF, Seq("user_id"), "left")
        .join(orgHierarchyDF, Seq("mdo_id"), "left")
        .withColumn("ministry_id", coalesce(col("ministry_id"), col("mdo_id"))) // Replace null ministry_id with mdo_id
        .groupBy("ministry_id")
        .agg(countDistinct("certificate_id").alias("event_certificate_count"))

      val contentCertificatesGeneratedSLWYdayDF = contentEnrolmentsDF
        .filter(col("first_completed_on") >= previousStart && col("first_completed_on") <= previousEnd)
        .filter(col("certificate_id").isNotNull)
        .join(userDetailsDF, Seq("user_id"), "left")
        .join(orgHierarchyDF, Seq("mdo_id"), "left")
        .withColumn("ministry_id", coalesce(col("ministry_id"), col("mdo_id")))
        .groupBy("ministry_id")
        .agg(count("*").alias("content_certificate_count"))

      val totalCertificatesGeneratedSLWYdayByOrgDF = eventCertificatesGeneratedSLWYdayDF
        .join(contentCertificatesGeneratedSLWYdayDF, Seq("ministry_id"), "outer")
        .withColumn("total_certificate_generatedYday_slw_count", coalesce(col("event_certificate_count"), lit(0)) +
          coalesce(col("content_certificate_count"), lit(0)))
        .filter(col("ministry_id").isNotNull)

      // mdo level filtering
      val eventCertificatesGeneratedSLWMdoYdayDF = eventsEnrolmentsDF
        .filter(col("completed_on_datetime") >= "2025-07-14 00:00:00" && col("completed_on_datetime") <= "2025-07-21 23:59:59")
        .filter(col("certificate_id").isNotNull)
        .join(userDetailsDF, Seq("user_id"), "left")
        .join(orgHierarchyDF, Seq("mdo_id"), "left")
        .withColumn("ministry_id", coalesce(col("ministry_id"), col("mdo_id"))) // Replace null ministry_id with mdo_id
        .groupBy("mdo_id")
        .agg(countDistinct("certificate_id").alias("event_certificate_count"))

      val contentCertificatesGeneratedSLWMdoYdayDF = contentEnrolmentsDF
        .filter(col("first_completed_on") >= "2025-07-14 00:00:00" && col("first_completed_on") <= "2025-07-21 23:59:59")
        .filter(col("certificate_id").isNotNull)
        .join(userDetailsDF, Seq("user_id"), "left")
        .join(orgHierarchyDF, Seq("mdo_id"), "left")
        .withColumn("ministry_id", coalesce(col("ministry_id"), col("mdo_id")))
        .groupBy("mdo_id")
        .agg(count("*").alias("content_certificate_count"))

      val totalCertificatesGeneratedSLWYdayByMdoDF = eventCertificatesGeneratedSLWMdoYdayDF
        .join(contentCertificatesGeneratedSLWMdoYdayDF, Seq("mdo_id"), "outer")
        .withColumn("total_certificate_generatedYday_slw_count", coalesce(col("event_certificate_count"), lit(0)) +
          coalesce(col("content_certificate_count"), lit(0)))
        .filter(col("mdo_id").isNotNull)

      val dfMinistryRenamed = totalCertificatesGeneratedSLWYdayByOrgDF
        .withColumnRenamed("ministry_id", "entity_id")

      val dfMDORenamed = totalCertificatesGeneratedSLWYdayByMdoDF
        .withColumnRenamed("mdo_id", "entity_id")

      // Step 2: Filter MDOs not present in Ministry
      val dfMDOFiltered = dfMDORenamed.join(dfMinistryRenamed, Seq("entity_id"), "left_anti")

      // Step 3: Union both
      val result = dfMinistryRenamed.union(dfMDOFiltered)

      Redis.dispatchDataFrame[Int]("dashboard_certificate_generated_yday_by_ministry_slw_count", result, "entity_id", "total_certificate_generatedYday_slw_count")

      // total enrolment stats starts
      val eventEnrolmentsInSLWDF = eventsEnrolmentsDF
        .filter(col("enrolled_on_datetime") >= stateLearningWeekStartString && col("enrolled_on_datetime") <= stateLearningWeekEndString)
        .join(userDetailsDF, Seq("user_id"), "left")
        .join(orgHierarchyDF, Seq("mdo_id"), "left")
        .withColumn("ministry_id", coalesce(col("ministry_id"), col("mdo_id")))
        .filter(col("ministry_id") =!= nlw_mdo_id)
        .groupBy("ministry_id")
        .agg(count("*").alias("event_enrolment_count"))

      val contentEnrolmentsInSLWDF = contentEnrolmentsDF
        .filter(col("enrolled_on") >= stateLearningWeekStartString && col("enrolled_on") <= stateLearningWeekEndString)
        .join(userDetailsDF, Seq("user_id"), "left")
        .join(orgHierarchyDF, Seq("mdo_id"), "left")
        .withColumn("ministry_id", coalesce(col("ministry_id"), col("mdo_id")))
        .filter(col("ministry_id") =!= nlw_mdo_id)
        .groupBy("ministry_id")
        .agg(count("*").alias("content_enrolment_count"))

      val totalEnrolmentsInSLWByMinistryDF = eventEnrolmentsInSLWDF
        .join(contentEnrolmentsInSLWDF, Seq("ministry_id"), "full_outer")
        .select(col("ministry_id"), coalesce(col("event_enrolment_count"), lit(0)).alias("event_enrolment_count"), coalesce(col("content_enrolment_count"), lit(0)).alias("content_enrolment_count"),
          (coalesce(col("event_enrolment_count"), lit(0)) + coalesce(col("content_enrolment_count"), lit(0))).alias("total_enrolments"))
        .filter(col("ministry_id").isNotNull)

      // total enrolment stats for other MDO start
      val eventEnrolmentsInSLWMdoDF = eventsEnrolmentsDF
        .filter(col("enrolled_on_datetime") >= "2025-07-14 00:00:00" && col("enrolled_on_datetime") <= "2025-07-21 23:59:59")
        .join(userDetailsDF, Seq("user_id"), "left")
        .join(orgHierarchyDF, Seq("mdo_id"), "left")
        .withColumn("ministry_id", coalesce(col("ministry_id"), col("mdo_id")))
        .filter(col("ministry_id") =!= nlw_mdo_id)
        .groupBy("mdo_id")
        .agg(count("*").alias("event_enrolment_count"))

      val contentEnrolmentsInSLWMdoDF = contentEnrolmentsDF
        .filter(col("enrolled_on") >= "2025-07-14 00:00:00" && col("enrolled_on") <= "2025-07-21 23:59:59")
        .join(userDetailsDF, Seq("user_id"), "left")
        .join(orgHierarchyDF, Seq("mdo_id"), "left")
        .withColumn("ministry_id", coalesce(col("ministry_id"), col("mdo_id")))
        .filter(col("ministry_id") =!= nlw_mdo_id)
        .groupBy("mdo_id")
        .agg(count("*").alias("content_enrolment_count"))

      val totalEnrolmentsInSLWByMdoDF = eventEnrolmentsInSLWMdoDF
        .join(contentEnrolmentsInSLWMdoDF, Seq("mdo_id"), "full_outer")
        .select(col("mdo_id"), coalesce(col("event_enrolment_count"), lit(0)).alias("event_enrolment_count"), coalesce(col("content_enrolment_count"), lit(0)).alias("content_enrolment_count"),
          (coalesce(col("event_enrolment_count"), lit(0)) + coalesce(col("content_enrolment_count"), lit(0))).alias("total_enrolments"))
        .filter(col("mdo_id").isNotNull)

      // for maharashtra
      val maharashtraEventEnrolments = eventsEnrolmentsDF
        .filter(col("enrolled_on_datetime") >= "2023-04-01 00:00:00" && col("enrolled_on_datetime") <= "2025-05-31 23:59:59")
        .join(userDetailsDF, Seq("user_id"), "left")
        .join(orgHierarchyDF, Seq("mdo_id"), "left")
        .withColumn("ministry_id", coalesce(col("ministry_id"), col("mdo_id")))
        .filter(col("ministry_id") === nlw_mdo_id)
        .groupBy("ministry_id")
        .agg(count("*").alias("event_enrolment_count"))

      val maharashtraContentEnrolments = contentEnrolmentsDF
        .filter(col("enrolled_on") >= "2023-04-01 00:00:00" && col("enrolled_on") <= "2025-05-31 23:59:59")
        .join(userDetailsDF, Seq("user_id"), "left")
        .join(orgHierarchyDF, Seq("mdo_id"), "left")
        .withColumn("ministry_id", coalesce(col("ministry_id"), col("mdo_id")))
        .filter(col("ministry_id") === nlw_mdo_id)
        .groupBy("ministry_id")
        .agg(count("*").alias("content_enrolment_count"))

      val maharashtraTotalEnrolments = maharashtraEventEnrolments.join(maharashtraContentEnrolments, Seq("ministry_id"), "full_outer")
        .select(
          col("ministry_id"),
          coalesce(col("event_enrolment_count"), lit(0)).alias("event_enrolment_count"),
          coalesce(col("content_enrolment_count"), lit(0)).alias("content_enrolment_count"),
          (coalesce(col("event_enrolment_count"), lit(0)) + coalesce(col("content_enrolment_count"), lit(0))).alias("total_enrolments"))
        .filter(col("ministry_id").isNotNull
        )

      // merging ministry and mdo data
      val enrolmentDFMinistryRenamed = totalEnrolmentsInSLWByMinistryDF
        .withColumnRenamed("ministry_id", "entity_id")

      val enrolmentDFMDORenamed = totalEnrolmentsInSLWByMdoDF
        .withColumnRenamed("mdo_id", "entity_id")

      // Step 2: Filter MDOs not present in Ministry
      val enrolmentDFMDOFiltered = enrolmentDFMDORenamed.join(enrolmentDFMinistryRenamed, Seq("entity_id"), "left_anti")

      // Step 3: Union both
      val enrolmentResultDF = enrolmentDFMinistryRenamed.union(enrolmentDFMDOFiltered)

      Redis.dispatchDataFrame[Int]("dashboard_total_enrolment_by_ministry_slw_count", enrolmentResultDF, "entity_id", "total_enrolments")
      //Redis.dispatchDataFrame[Int]("dashboard_total_enrolment_by_ministry_slw_count", maharashtraTotalEnrolments, "ministry_id", "total_enrolments")
      // total enrolments stats ends

      // certificate generated stats starts
      val eventCertificatesGeneratedInSLWDF = eventsEnrolmentsDF
        .filter(col("completed_on_datetime") >= stateLearningWeekStartString && col("completed_on_datetime") <= stateLearningWeekEndString)
        .filter(col("certificate_id").isNotNull)
        .join(userDetailsDF, Seq("user_id"), "left")
        .join(orgHierarchyDF, Seq("mdo_id"), "left")
        .withColumn("ministry_id", coalesce(col("ministry_id"), col("mdo_id")))
        .filter(col("ministry_id") =!= nlw_mdo_id)
        .groupBy("ministry_id")
        .agg(countDistinct("certificate_id").alias("event_certificate_count"))

      val contentCertificatesGeneratedInSLWDF = contentEnrolmentsDF
        .filter(col("first_completed_on") >= stateLearningWeekStartString && col("first_completed_on") <= stateLearningWeekEndString)
        .filter(col("certificate_id").isNotNull)
        .join(userDetailsDF, Seq("user_id"), "left")
        .join(orgHierarchyDF, Seq("mdo_id"), "left")
        .withColumn("ministry_id", coalesce(col("ministry_id"), col("mdo_id")))
        .filter(col("ministry_id") =!= nlw_mdo_id)
        .groupBy("ministry_id")
        .agg(count("*").alias("content_certificate_count"))

      val totalCertificatesGeneratedInSLWByMinistryDF = eventCertificatesGeneratedInSLWDF
        .join(contentCertificatesGeneratedInSLWDF, Seq("ministry_id"), "full_outer")
        .select(col("ministry_id"), coalesce(col("event_certificate_count"), lit(0)).alias("event_certificate_count"), coalesce(col("content_certificate_count"), lit(0)).alias("content_certificate_count"),
          (coalesce(col("event_certificate_count"), lit(0)) + coalesce(col("content_certificate_count"), lit(0))).alias("total_certificates"))
        .filter(col("ministry_id").isNotNull)

      // MDO certificate generated stats starts
      val eventCertificatesGeneratedInSLWMdoDF = eventsEnrolmentsDF
        .filter(col("completed_on_datetime") >= "2025-07-14 00:00:00" && col("completed_on_datetime") <= "2025-07-21 23:59:59")
        .filter(col("certificate_id").isNotNull)
        .join(userDetailsDF, Seq("user_id"), "left")
        .join(orgHierarchyDF, Seq("mdo_id"), "left")
        .withColumn("ministry_id", coalesce(col("ministry_id"), col("mdo_id")))
        .filter(col("ministry_id") =!= nlw_mdo_id)
        .groupBy("mdo_id")
        .agg(countDistinct("certificate_id").alias("event_certificate_count"))

      val contentCertificatesGeneratedInSLWMdoDF = contentEnrolmentsDF
        .filter(col("first_completed_on") >= "2025-07-14 00:00:00" && col("first_completed_on") <= "2025-07-21 23:59:59")
        .filter(col("certificate_id").isNotNull)
        .join(userDetailsDF, Seq("user_id"), "left")
        .join(orgHierarchyDF, Seq("mdo_id"), "left")
        .withColumn("ministry_id", coalesce(col("ministry_id"), col("mdo_id")))
        .filter(col("ministry_id") =!= nlw_mdo_id)
        .groupBy("mdo_id")
        .agg(count("*").alias("content_certificate_count"))

      val totalCertificatesGeneratedInSLWByMdoDF = eventCertificatesGeneratedInSLWMdoDF
        .join(contentCertificatesGeneratedInSLWMdoDF, Seq("mdo_id"), "full_outer")
        .select(col("mdo_id"), coalesce(col("event_certificate_count"), lit(0)).alias("event_certificate_count"), coalesce(col("content_certificate_count"), lit(0)).alias("content_certificate_count"),
          (coalesce(col("event_certificate_count"), lit(0)) + coalesce(col("content_certificate_count"), lit(0))).alias("total_certificates"))
        .filter(col("mdo_id").isNotNull)

      //maharashtra certificates generated
      val maharshtraEventCertificates = eventsEnrolmentsDF
        .filter(col("completed_on_datetime") >= "2023-04-01 00:00:00" && col("completed_on_datetime") <= "2025-05-31 23:59:59")
        .filter(col("certificate_id").isNotNull)
        .join(userDetailsDF, Seq("user_id"), "left")
        .join(orgHierarchyDF, Seq("mdo_id"), "left")
        .withColumn("ministry_id", coalesce(col("ministry_id"), col("mdo_id")))
        .filter(col("ministry_id") === nlw_mdo_id)
        .groupBy("ministry_id")
        .agg(countDistinct("certificate_id").alias("event_certificate_count"))

      val maharshtraContentCertificates = contentEnrolmentsDF
        .filter(col("first_completed_on") >= "2023-04-01 00:00:00" && col("first_completed_on") <= "2025-05-31 23:59:59")
        .filter(col("certificate_id").isNotNull)
        .join(userDetailsDF, Seq("user_id"), "left")
        .join(orgHierarchyDF, Seq("mdo_id"), "left")
        .withColumn("ministry_id", coalesce(col("ministry_id"), col("mdo_id")))
        .filter(col("ministry_id") === nlw_mdo_id)
        .groupBy("ministry_id")
        .agg(count("*").alias("content_certificate_count"))

      val maharashtraTotalCertificates = maharshtraEventCertificates.join(maharshtraContentCertificates, Seq("ministry_id"), "full_outer")
        .select(
          col("ministry_id"),
          coalesce(col("event_certificate_count"), lit(0)).alias("event_certificate_count"),
          coalesce(col("content_certificate_count"), lit(0)).alias("content_certificate_count"),
          (coalesce(col("event_certificate_count"), lit(0)) + coalesce(col("content_certificate_count"), lit(0))).alias("total_certificates"))
        .filter(col("ministry_id").isNotNull)

      // merging ministry and mdo data
      val certificateDFMinistryRenamed = totalCertificatesGeneratedInSLWByMinistryDF
        .withColumnRenamed("ministry_id", "entity_id")

      val certificateDFMDORenamed = totalCertificatesGeneratedInSLWByMdoDF
        .withColumnRenamed("mdo_id", "entity_id")

      // Step 2: Filter MDOs not present in Ministry
      val certificateDFMDOFiltered = certificateDFMDORenamed.join(certificateDFMinistryRenamed, Seq("entity_id"), "left_anti")

      // Step 3: Union both
      val certificateResultDF = certificateDFMinistryRenamed.union(certificateDFMDOFiltered)


      Redis.dispatchDataFrame[Int]("dashboard_certificates_generated_by_ministry_slw_count", certificateResultDF, "entity_id", "total_certificates")
      //Redis.dispatchDataFrame[Int]("dashboard_certificates_generated_by_ministry_slw_count", maharashtraTotalCertificates, "ministry_id", "total_certificates")
      // certificate generated stats ends

     
      val slwStartDate = stateLearningWeekStartString.split(" ")(0)
      val slwEndDate = stateLearningWeekEndString.split(" ")(0)
      val slwDateConditions = s"""{"range": {"startDate": {"gte": "${slwStartDate}", "lte": "${slwEndDate}"}}}"""
      val objectType = Seq("Event")
      val shouldClauseRequired = objectType.map(pc => s"""{"match":{"objectType.raw":"${pc}"}}""").mkString(",")
      val fieldsRequired = Seq("identifier", "name", "objectType", "resourceType", "status", "startDate", "startTime", "duration", "registrationLink" ,"createdFor", "recordedLinks","resourceTypeDetails")
      val arrayFieldsRequired = Seq("createdFor","recordedLinks")
      val fieldsClauseRequired = fieldsRequired.map(f => s""""${f}"""").mkString(",")
      val eventQuery = s"""{"_source":[${fieldsClauseRequired}],"query":{"bool":{"must": [${slwDateConditions}], "should":[${shouldClauseRequired}]}}}"""
      val eventDataDF = elasticSearchDataFrame(conf.sparkElasticsearchConnectionHost, "compositesearch", eventQuery, fieldsRequired, arrayFieldsRequired)
      val filteredDF = eventDataDF.filter(col("resourceType") === "Rajya Karmayogi Saptah")
      
      // resourceTypeDetails.stateOrMinistryId is an array, explode it first
      val explodedDF = filteredDF.select(
        col("identifier"),
        explode(col("resourceTypeDetails.stateOrMinistryId")).alias("stateOrMinistryId")
      )

      // Group by exploded createdFor and count distinct identifiers
      val publishedEventsCountByCreatedFor = explodedDF
        .groupBy("stateOrMinistryId")
        .agg(countDistinct("identifier").alias("event_count"))
        .orderBy(desc("event_count"))

      publishedEventsCountByCreatedFor.show(false)

      // adding filter to temporarly handle bihar mdo
      val filteredRows = publishedEventsCountByCreatedFor
        .filter(col("stateOrMinistryId") === "01358339172689510459")
        .collect()

      val publishedEventsCount = if (filteredRows.nonEmpty) {
        filteredRows.head.getAs[Long]("event_count")
      } else {
        0L // Default value when no matching rows
      }

      val mdoEventCountDF = publishedEventsCountByCreatedFor
        .select(
          col("stateOrMinistryId").alias("mdoId"),
          col("event_count")
        )

      Redis.dispatchDataFrame[Int]("dashboard_events_published_by_ministry_count", mdoEventCountDF, "mdoId", "event_count")
      Redis.update("dashboard_events_published_by_ministry_slw_count", publishedEventsCount.toString)
      // events published stats ends

      val userEventCertificatesDF = eventsEnrolmentsDF
        .filter(col("completed_on_datetime") >= stateLearningWeekStartString && col("completed_on_datetime") <= stateLearningWeekEndString)
        .filter(col("certificate_id").isNotNull)
        .groupBy("user_id")
        .agg(countDistinct("certificate_id").alias("event_certificate_count"))

      val userContentCertificatesDF = contentEnrolmentsDF
        .filter(col("first_completed_on") >= stateLearningWeekStartString && col("first_completed_on") <= stateLearningWeekEndString)
        .filter(col("certificate_id").isNotNull)
        .groupBy("user_id")
        .agg(count("*").alias("content_certificate_count"))

      val userEventLearningHoursDF = eventsEnrolmentsDF
        .filter(col("completed_on_datetime") >= stateLearningWeekStartString && col("completed_on_datetime") <= stateLearningWeekEndString)
        .filter(col("certificate_id").isNotNull)
        .join(eventsDF.withColumnRenamed("duration", "event_complete_duration"), Seq("event_id"), "left")
        .withColumn("event_duration_hours", timeToHoursUDF(col("event_complete_duration"))) // Convert directly from eventsEnrolmentsDF
        .groupBy("user_id")
        .agg(sum(coalesce(col("event_duration_hours"), lit(0))).alias("event_learning_hours"))

      val userContentLearningHoursDF = contentEnrolmentsDF
        .filter(col("first_completed_on") >= stateLearningWeekStartString && col("first_completed_on") <= stateLearningWeekEndString) // Fixed end date condition
        .filter(col("certificate_id").isNotNull)
        .join(contentDF.filter(col("content_sub_type").isin("Course", "Moderated Course")), Seq("content_id"), "inner")
        .withColumn("content_duration_hours", timeToHoursUDF(col("content_duration"))) // Convert after join
        .groupBy("user_id")
        .agg(sum(coalesce(col("content_duration_hours"), lit(0))).alias("content_learning_hours"))

      val userTotalCertificatesDF = userEventCertificatesDF
        .join(userContentCertificatesDF, Seq("user_id"), "full_outer")
        .select(col("user_id"), (coalesce(col("event_certificate_count"), lit(0)) + coalesce(col("content_certificate_count"), lit(0))).alias("total_certificates"))

      val userTotalLearningHoursDF = userEventLearningHoursDF
        .join(userContentLearningHoursDF, Seq("user_id"), "full_outer")
        .select(
          col("user_id"),
          coalesce(col("event_learning_hours"), lit(0)).alias("event_learning_hours"),
          coalesce(col("content_learning_hours"), lit(0)).alias("content_learning_hours"),
          round(coalesce(col("event_learning_hours"), lit(0)) + coalesce(col("content_learning_hours"), lit(0)), 2).alias("total_learning_hours"))


      val karmaPointsDataDF = cache.load("userKarmaPoints")
        .filter(col("credit_date") >= stateLearningWeekStartString && col("credit_date") <= stateLearningWeekEndString)
        .groupBy(col("userid")).agg(sum(col("points")).alias("total_points"), max(col("credit_date")).alias("last_credit_date"))

      val (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()

      val userOrgData = userOrgDF.select(
          userOrgDF("userID").alias("userid"),
          userOrgDF("userOrgID").alias("org_id"),
          userOrgDF("fullName").alias("fullname"),
          userOrgDF("userOrgName").alias("org_name"),
          userOrgDF("professionalDetails.designation").alias("designation"),
          userOrgDF("userProfileImgUrl").alias("profile_image"))


      val userLeaderBoardDataDF = userOrgData.join(karmaPointsDataDF, Seq("userid"), "left")
        .filter(col("org_id") =!= "")
        .select(userOrgData("userid").alias("user_id"),
          userOrgData("org_id"),
          userOrgData("fullname"),
          userOrgData("designation"),
          userOrgData("org_name"),
          userOrgData("profile_image"),
          karmaPointsDataDF("total_points"),
          karmaPointsDataDF("last_credit_date"))

      val windowSpecRank = Window.partitionBy("org_id").orderBy(desc("total_points"))
      val userLeaderBoardOrderedDataDF = userLeaderBoardDataDF.withColumn("rank", dense_rank().over(windowSpecRank))
      val windowSpecRow = Window.partitionBy("org_id").orderBy(col("rank"), col("last_credit_date").asc)
      val finalUserLeaderBoardDataDF = userLeaderBoardOrderedDataDF.withColumn("row_num", row_number.over(windowSpecRow))

      val userStatsDF = userTotalCertificatesDF
        .join(userTotalLearningHoursDF, Seq("user_id"), "full_outer")
        .select(
          col("user_id"),
          coalesce(col("total_certificates"), lit(0)).alias("count"),
          coalesce(col("total_learning_hours"), lit(0.0)).alias("total_learning_hours"))

      val userStatsDetailedDF = userStatsDF.join(finalUserLeaderBoardDataDF, Seq("user_id"), "right")

      val selectedColUserLeaderboardDF = userStatsDetailedDF
        .select(
          col("user_id").alias("userid"),
          col("org_id"),
          col("fullname"),
          col("designation"),
          col("profile_image"),
          coalesce(col("total_points"), lit(0)).alias("total_points"),
          coalesce(col("rank"), lit(0)).alias("rank"),
          col("row_num"),
          col("count"),
          col("total_learning_hours"),
          col("last_credit_date"))
        .dropDuplicates("userid")

      val alignedDF = selectedColUserLeaderboardDF
        .withColumn("last_credit_date", col("last_credit_date").cast("string"))
        .withColumn("total_learning_hours", col("total_learning_hours").cast("string"))
        .withColumn("count", col("count").cast("int"))
      truncateWarehouseTable(conf.dwNLWUserLeaderboardTable, appPostgresUrl)
      saveDataframeToPostgresTable_With_Append(selectedColUserLeaderboardDF, appPostgresUrl, conf.dwNLWUserLeaderboardTable, conf.appPostgresUsername, conf.appPostgresCredential)

      val userWithMinistryForTopLearnersDF = selectedColUserLeaderboardDF
        .join(orgHierarchyDF, selectedColUserLeaderboardDF("org_id") === orgHierarchyDF("mdo_id"), "left")
        .select(
          col("userid").alias("user_id"),
          col("fullname"),
          col("designation"),
          col("profile_image"),
          col("org_id"),
          col("mdo_name").alias("org_name"),
          col("total_points"),
          col("total_learning_hours"))

      val ministryWindowSpec = Window.partitionBy("org_id").orderBy(col("total_learning_hours").desc)
      val ministryLearnersWithRowNumDF = userWithMinistryForTopLearnersDF.withColumn("row_num", row_number().over(ministryWindowSpec))
      val ministryTopLearnersFilteredDF = ministryLearnersWithRowNumDF
        .filter(col("row_num") <= 10)
        .select(
          col("user_id").alias("userid"),
          col("org_id"),
          col("fullname"),
          col("profile_image"),
          col("org_name"),
          col("designation"),
          col("total_points"),
          col("row_num"),
          col("total_learning_hours")
        )
      truncateWarehouseTable(conf.dwSLWMdoTopLearnerTable, appPostgresUrl)
      saveDataframeToPostgresTable_With_Append(ministryTopLearnersFilteredDF, appPostgresUrl, conf.dwSLWMdoTopLearnerTable, conf.appPostgresUsername, conf.appPostgresCredential)

      val filteredOrgHierarchyDF = orgHierarchyDF
        .filter(col("ministry_id").isNotNull) // Ensure ministry_id is present
        .withColumn("parent_id", col("ministry_id")) // Set ministry_id as parent_id
        .withColumn("ministry_name", col("ministry"))
        .select("parent_id", "mdo_id", "mdo_name", "ministry_name")

      // Step 2: Group by parent_id, dept_id, department & collect all unique MDOs
      val departmentToMDOsDF = filteredOrgHierarchyDF
        .groupBy("mdo_id", "mdo_name")

      val userWithDeptDF = selectedColUserLeaderboardDF
        .join(filteredOrgHierarchyDF, selectedColUserLeaderboardDF("org_id") === filteredOrgHierarchyDF("mdo_id"), "inner")
        .select(
          col("userid"),
          col("parent_id"),
          col("mdo_name").alias("org_name"),
          col("mdo_id").alias("org_id"),
          coalesce(col("total_learning_hours"), lit(0)).alias("total_learning_hours"))

      val userWithDeptFilteredDF = userWithDeptDF.filter(col("total_learning_hours") >= 4)
      val ministryWiseDeptDF = userWithDeptFilteredDF.groupBy("parent_id", "org_id", "org_name").agg(countDistinct("userid").alias("active_users_count"))
      val bucketRegex = """(\d+)-(\d+)-(\w+)""".r
      val aboveRegex = """above\s+(\d+)-(\w+)""".r

      val conditions: Seq[(Column, String)] = conf.sizeBucketString.split(",").flatMap {
        case bucketRegex(start, end, label) =>
          Some((col("total_users") >= start.toInt && col("total_users") <= end.toInt, label))
        case aboveRegex(min, label) =>
          Some((col("total_users") > min.toInt, label))
        case _ => None
      }

      val sizeColumn: Column = conditions.foldLeft(lit(null: String)) {
        case (colExpr, (cond, label)) => when(cond, label).otherwise(colExpr)
      }

      val ministryWiseDeptWithSizeDF = ministryWiseDeptDF.join(userWithDeptDF.groupBy("parent_id", "org_id").agg(countDistinct("userid")
        .alias("total_users")), Seq("parent_id", "org_id"), "left").withColumn("size", sizeColumn)

      val windowSpec = Window.partitionBy("parent_id", "size").orderBy(col("active_users_count").desc, rand())

      val rankedDF = ministryWiseDeptWithSizeDF.withColumn("row_num", row_number().over(windowSpec))

      val finalDF = rankedDF.select(col("parent_id").cast(StringType), col("org_id").cast(StringType), col("org_name").cast(StringType),
        col("size").cast(StringType), col("total_users").cast(IntegerType),
        coalesce(col("active_users_count"), lit(0)).cast(IntegerType).alias("total_learning_hours"), col("row_num").cast(IntegerType))

      truncateWarehouseTable(conf.dwSLWMdoLeaderboardTable, appPostgresUrl)
      saveDataframeToPostgresTable_With_Append(finalDF, appPostgresUrl, conf.dwSLWMdoLeaderboardTable, conf.appPostgresUsername, conf.appPostgresCredential)

    } catch {
      case e: Exception =>
        println(s"Error occurred during NationalLearningWeekModel processing: ${e.getMessage}", e)
        System.exit(1)
    }
  }
}
