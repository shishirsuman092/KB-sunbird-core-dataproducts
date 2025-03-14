package org.ekstep.analytics.dashboard.nationallearningweek

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
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

    def timeToHoursUDF: UserDefinedFunction = udf((timeStr: String) => {
      if (timeStr != null && timeStr.matches("\\d{1,2}:\\d{2}:\\d{2}")) {
        val parts = timeStr.split(":").map(_.toDouble)
        parts(0) + parts(1) / 60 + parts(2) / 3600
      } else {
        0.0
      }})
    val stateLearningWeekStartString = conf.stateLearningWeekStart
    val stateLearningWeekEndString = conf.stateLearningWeekEnd
    val zoneOffset = ZoneOffset.ofHoursMinutes(5, 30)

    val currentDate = LocalDate.now()
    val previousDayStart = currentDate.minusDays(1).atStartOfDay().atOffset(zoneOffset)
    val previousDayEnd = currentDate.atStartOfDay().minusSeconds(1).atOffset(zoneOffset)
    val eventsDateTimeFormatter=DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
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
      .filter(col("status") === "completed")
      .filter(col("enrolled_on_datetime") >= previousStart && col("enrolled_on_datetime") <= previousEnd)
      .filter(col("certificate_id").isNotNull)
      .join(userDetailsDF,Seq("user_id"), "left")
      .join(orgHierarchyDF, Seq("mdo_id"), "left")
      .groupBy("ministry_id")
      .agg(countDistinct("certificate_id").alias("event_certificate_count"))

    val contentCertificatesGeneratedSLWYdayDF = contentEnrolmentsDF
      .filter(col("last_certificate_generated_on") >= previousStart && col("last_certificate_generated_on") <= previousEnd)
      .filter(col("certificate_generated") === "Yes")
      .join(userDetailsDF,Seq("user_id"), "left")
      .join(orgHierarchyDF, Seq("mdo_id"), "left")
      .groupBy("ministry_id")
      .agg(count("*").alias("content_certificate_count"))


    val totalCertificatesGeneratedSLWYdayByOrgDF = eventCertificatesGeneratedSLWYdayDF
      .join(contentCertificatesGeneratedSLWYdayDF, Seq("ministry_id"), "outer")
      .withColumn("total_certificate_generatedYday_slw_count", col("event_certificate_count") + col("content_certificate_count"))
      .filter(col("ministry_id").isNotNull)

    Redis.dispatchDataFrame[Int]("dashboard_certificate_generated_yday_by_ministry_slw_count", totalCertificatesGeneratedSLWYdayByOrgDF, "ministry_id", "total_certificate_generatedYday_slw_count")


    val eventEnrolmentsInSLWDF = eventsEnrolmentsDF
      .filter(col("enrolled_on_datetime") >= stateLearningWeekStartString && col("enrolled_on_datetime") <= stateLearningWeekEndString)
      .join(userDetailsDF, Seq("user_id"), "left")
      .join(orgHierarchyDF, Seq("mdo_id"), "left")
      .groupBy("ministry_id")
      .agg(countDistinct("certificate_id").alias("event_enrolment_count"))


    val contentEnrolmentsInSLWDF = contentEnrolmentsDF
      .filter(col("enrolled_on") >= stateLearningWeekStartString && col("enrolled_on") <= stateLearningWeekEndString)
      .join(userDetailsDF,Seq("user_id"), "left")
      .join(orgHierarchyDF, Seq("mdo_id"), "left")
      .groupBy("ministry_id")
      .agg(count("*").alias("content_enrolment_count"))

    val totalEnrolmentsInSLWByMinistryDF = eventEnrolmentsInSLWDF
      .join(contentEnrolmentsInSLWDF, Seq("ministry_id"), "full_outer")
      .select(col("ministry_id"), coalesce(col("event_enrolment_count"), lit(0)).alias("event_enrolment_count"), coalesce(col("content_enrolment_count"), lit(0)).alias("content_enrolment_count"),
        (coalesce(col("event_enrolment_count"), lit(0)) + coalesce(col("content_enrolment_count"), lit(0))).alias("total_enrolments"))
      .filter(col("ministry_id").isNotNull)

    Redis.dispatchDataFrame[Int]("dashboard_total_enrolment_by_ministry_slw_count", totalEnrolmentsInSLWByMinistryDF, "ministry_id", "total_enrolments")


    val eventCertificatesGeneratedInSLWDF = eventsEnrolmentsDF
      .filter(col("status") === "completed")
      .filter(col("enrolled_on_datetime") >= stateLearningWeekStartString && col("enrolled_on_datetime") <= stateLearningWeekEndString)
      .filter(col("certificate_id").isNotNull)
      .join(userDetailsDF, Seq("user_id"), "left")
      .join(orgHierarchyDF, Seq("mdo_id"), "left")
      .groupBy("ministry_id")
      .agg(countDistinct("certificate_id").alias("event_certificate_count"))



    val contentCertificatesGeneratedInSLWDF = contentEnrolmentsDF
      .filter(col("last_certificate_generated_on") >= stateLearningWeekStartString && col("last_certificate_generated_on") <= stateLearningWeekEndString)
      .filter(col("certificate_generated") === "Yes")
      .join(userDetailsDF,Seq("user_id"), "left")
      .join(orgHierarchyDF, Seq("mdo_id"), "left")
      .groupBy("ministry_id")
      .agg(count("*").alias("content_certificate_count"))

    val totalCertificatesGeneratedInSLWByMinistryDF = eventCertificatesGeneratedInSLWDF
      .join(contentCertificatesGeneratedInSLWDF, Seq("ministry_id"), "full_outer")
      .select(col("ministry_id"), coalesce(col("event_certificate_count"), lit(0)).alias("event_certificate_count"), coalesce(col("content_certificate_count"), lit(0)).alias("content_certificate_count"),
        (coalesce(col("event_certificate_count"), lit(0)) + coalesce(col("content_certificate_count"), lit(0))).alias("total_certificates"))
      .filter(col("ministry_id").isNotNull)

    Redis.dispatchDataFrame[Int]("dashboard_certificates_generated_by_ministry_slw_count", totalCertificatesGeneratedInSLWByMinistryDF, "ministry_id", "total_certificates")

    val slwStartDate = stateLearningWeekStartString.split(" ")(0)
    val slwEndDate = stateLearningWeekEndString.split(" ")(0)
    val slwDateConditions = s"""{"range": {"startDate": {"gte": "${slwStartDate}", "lte": "${slwEndDate}"}}}"""
    val objectType = Seq("Event")
    val shouldClauseRequired = objectType.map(pc => s"""{"match":{"objectType.raw":"${pc}"}}""").mkString(",")
    val fieldsRequired = Seq("identifier", "name", "objectType", "status", "startDate", "startTime", "duration", "registrationLink" ,"createdFor", "recordedLinks")
    val arrayFieldsRequired = Seq("createdFor","recordedLinks")
    val fieldsClauseRequired = fieldsRequired.map(f => s""""${f}"""").mkString(",")
    val eventQuery = s"""{"_source":[${fieldsClauseRequired}],"query":{"bool":{"should":[${shouldClauseRequired}]}}}"""
    val eventDataDF = elasticSearchDataFrame(conf.sparkElasticsearchConnectionHost, "compositesearch", eventQuery, fieldsRequired, arrayFieldsRequired)
    val eventsPublishedInSLWWithProviderSDF = eventDataDF
      .withColumn("event_provider_mdo_id", explode_outer(col("createdFor")))
      .filter(col("startDate") >= stateLearningWeekStartString && col("startDate") <= stateLearningWeekEndString)
    val eventsPublishedByMinistrySLWDF = eventsPublishedInSLWWithProviderSDF
      .join(orgHierarchyDF, eventsPublishedInSLWWithProviderSDF("event_provider_mdo_id") === orgHierarchyDF("mdo_id"), "left")
      .groupBy("ministry_id").agg(countDistinct("identifier").alias("events_published_count")).filter(col("ministry_id").isNotNull)


    Redis.dispatchDataFrame[Int]("dashboard_events_published_by_ministry_slw_count", eventsPublishedByMinistrySLWDF, "ministry_id", "events_published_count")

    val userEventCertificatesDF = eventsEnrolmentsDF.filter(col("status") === "completed")
      .filter(col("enrolled_on_datetime") >= stateLearningWeekStartString && col("enrolled_on_datetime") <= stateLearningWeekEndString)
      .filter(col("certificate_id").isNotNull)
      .groupBy("user_id")
      .agg(countDistinct("certificate_id").alias("event_certificate_count"))

    val userContentCertificatesDF = contentEnrolmentsDF
      .filter(col("last_certificate_generated_on") >= stateLearningWeekStartString && col("last_certificate_generated_on") <= stateLearningWeekStartString)
      .filter(col("certificate_generated") === "Yes")
      .groupBy("user_id")
      .agg(count("*").alias("content_certificate_count"))

    val userEventLearningHoursDF = eventsEnrolmentsDF
      .filter(col("enrolled_on_datetime") >= stateLearningWeekStartString && col("enrolled_on_datetime") <= stateLearningWeekEndString)
      .filter(col("status") === "completed")
      .join(eventsDF, Seq("event_id"), "left")
      .withColumn("event_duration_hours", timeToHoursUDF(col("duration"))) // Convert directly from eventsEnrolmentsDF
      .groupBy("user_id")
      .agg(sum(coalesce(col("event_duration_hours"), lit(0))).alias("event_learning_hours"))

    val userContentLearningHoursDF = contentEnrolmentsDF
      .filter(col("enrolled_on") >= stateLearningWeekStartString && col("enrolled_on") <= stateLearningWeekEndString) // Fixed end date condition
      .filter(col("user_consumption_status") === "completed")
      .join(contentDF, Seq("content_id"), "left") // Join first to get content_duration
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

    val userOrgData = userOrgDF.join(userDF, userOrgDF("userID") === userDF("userID"), "inner")
      .select(
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

    val userStatsDetailedDF = userStatsDF.join(finalUserLeaderBoardDataDF, Seq("user_id"), "full_outer")

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

    writeToCassandra(selectedColUserLeaderboardDF, conf.cassandraUserKeyspace, conf.cassandraNLWUserLeaderboardTable)
    //println("This is doneee")

    val userWithMinistryForTopLearnersDF = selectedColUserLeaderboardDF
      .join(orgHierarchyDF, selectedColUserLeaderboardDF("org_id") === orgHierarchyDF("mdo_id"), "left")
      .filter(col("ministry_id").isNotNull)
      .select(
        col("userid").alias("user_id"),
        col("fullname"),
        col("designation"),
        col("profile_image"),
        col("org_id"),
        col("mdo_name").alias("org_name"),
        col("total_points"),
        col("total_learning_hours"),
        col("ministry_id"))

    val ministryWindowSpec = Window.partitionBy("ministry_id").orderBy(col("total_learning_hours").desc)
    val ministryLearnersWithRowNumDF = userWithMinistryForTopLearnersDF.withColumn("row_num", row_number().over(ministryWindowSpec))
    val ministryTopLearnersFilteredDF = ministryLearnersWithRowNumDF
      .filter(col("row_num") <= 10)
      .select(
        col("user_id").alias("userid"),
        coalesce(col("ministry_id"), col("org_id")).alias("org_id"),
        col("fullname"),
        col("profile_image"),
        col("org_name"),
        col("designation"),
        col("total_points"),
        col("row_num"),
        col("total_learning_hours")
      )

    // Write to the new Cassandra table
    writeToCassandra(ministryTopLearnersFilteredDF, conf.cassandraUserKeyspace, conf.cassandraSLWMdoTopLearnerTable)
   // println("This is done 2")

    val userWithMinistryDF = selectedColUserLeaderboardDF
      .join(orgHierarchyDF, selectedColUserLeaderboardDF("org_id") === orgHierarchyDF("mdo_id"), "left")
      .select(
        col("userid"),
        col("org_id"),
        col("mdo_name").alias("org_name"),
        col("total_learning_hours"),
        orgHierarchyDF("ministry_id").alias("parent_id"))


    // Step 3: Group by ministry_id (parent_id) and org_id to count users
    val ministryWiseOrgDF = userWithMinistryDF
      .groupBy("parent_id", "org_id", "org_name")
      .agg(
        countDistinct("userid").alias("total_users"))

    // Step 4: Assign organization size based on total_users
    val sizedDF = ministryWiseOrgDF.withColumn("size", when(col("total_users") > 200, "M").otherwise("S"))

    // Step 5: Rank organizations within each ministry_id (parent_id)
    val windowSpec = Window.partitionBy("parent_id", "size")
      .orderBy(col("total_users").desc)

    val rankedDF = sizedDF.withColumn("row_num", row_number().over(windowSpec))

    // Step 6: Select the required columns
    val finalDF = rankedDF.select(
      col("parent_id"),       // Ministry ID as parent_id
      col("org_id"),
      col("size"),
      col("total_users"),
      col("org_name"),
      col("row_num")
    )
    val selectedColMdoLeaderboardDF = rankedDF.select(coalesce(col("parent_id"), col("org_id")).alias("parent_id"), col("org_id"), col("org_name"), col("size"), col("total_users"),col("row_num"))
    writeToCassandra(selectedColMdoLeaderboardDF, conf.cassandraUserKeyspace, conf.cassandraSLWMdoLeaderboardTable)
    //println("This is done 3")
  }
}

