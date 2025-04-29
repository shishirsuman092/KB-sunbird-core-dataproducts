package org.ekstep.analytics.dashboard.dsr

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{MapType, StringType}
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import java.time.{LocalDate, ZoneOffset}
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig, Redis}
import org.ekstep.analytics.framework.FrameworkContext

object DSRComputationModel extends AbsDashboardModel {

  implicit val className: String = "package org.ekstep.analytics.dashboard.dsr.DSRComputationModel"
  override def name() = "DSRComputationModel"
  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    try{
      val orgDF = cache.load("orgHierarchy")
      val eventsDF = cache.load("eventDetails")
      val userDF = warehouseCache.load(conf.dwUserTable)
      val eventsEnrolmentDataDF = cache.load("eventEnrolmentDetails")
      val contentEnrolmentDataDF =  warehouseCache.load(conf.dwEnrollmentsTable)
      val contentDF = warehouseCache.load(conf.dwCourseTable)
      val stateList = Seq("ANDAMAN and NICOBAR", "ANDHRA PRADESH", "ARUNACHAL PRADESH", "ASSAM", "BIHAR",
        "CHANDIGARH", "CHHATTISGARH", "Dadra and Nagar Haveli and Daman and Diu",
        "DELHI", "GOA", "GUJARAT", "HARYANA", "HIMACHAL PRADESH", "JAMMU and KASHMIR",
        "JHARKHAND", "KARNATAKA", "KERALA", "LADAKH", "LAKSHADWEEP", "MADHYA PRADESH",
        "MAHARASHTRA", "MANIPUR", "MEGHALAYA", "MIZORAM", "NAGALAND", "ODISHA",
        "PUDUCHERRY", "PUNJAB", "RAJASTHAN", "SIKKIM", "TAMIL NADU", "TELANGANA",
        "TRIPURA", "UTTAR PRADESH", "UTTARAKHAND", "WEST BENGAL")

      val loginSchema = StructType(Seq(StructField("user_id", StringType, nullable = true)))

      val userWithOrgDF = userDF.join(orgDF, Seq("mdo_id"), "inner")
      val activeUsersDF = userWithOrgDF.filter(col("status") === 1)
      val enrichedEnrolmentsDF = contentEnrolmentDataDF.join(userWithOrgDF, Seq("user_id"), "inner")
      val totalEnrolments = enrichedEnrolmentsDF.count()
      println(totalEnrolments)
      val stateEnrolmentsCount = enrichedEnrolmentsDF.filter(col("ministry").isin(stateList: _*) || col("mdo_name").isin(stateList: _*)).count()
      val centralEnrolmentsCount = totalEnrolments - stateEnrolmentsCount
      println("state enrolments : "+ stateEnrolmentsCount)
      println("central enrolments : "+ centralEnrolmentsCount)
      Redis.update("dashboard_state_content_enrolments", stateEnrolmentsCount.toString)
      Redis.update("dashboard_central_content_enrolments", centralEnrolmentsCount.toString)

      val stateUniqueUsersEnroledCount = enrichedEnrolmentsDF.filter(col("ministry").isin(stateList: _*) || col("mdo_name").isin(stateList: _*)).agg(countDistinct("user_id").as("unique_state_users")).first().getLong(0)
      val centralUniqueUsersEnroledCount = enrichedEnrolmentsDF.filter(!(col("ministry").isin(stateList: _*) || col("mdo_name").isin(stateList: _*))).agg(countDistinct("user_id").as("unique_central_users")).first().getLong(0)
      println("state unique users enrolled : "+ stateUniqueUsersEnroledCount)
      println("central unique users enrolled : "+ centralUniqueUsersEnroledCount)
      Redis.update("dashboard_state_unique_users_enrolled", stateUniqueUsersEnroledCount.toString)
      Redis.update("dashboard_central_unique_users_enrolled", centralUniqueUsersEnroledCount.toString)


      val enrichedCompletedDF = contentEnrolmentDataDF.filter(col("certificate_id").isNotNull).join(userWithOrgDF, Seq("user_id"), "inner")
      val totalCompletedCount = enrichedCompletedDF.count()
      println(totalCompletedCount)
      val stateCompletedCount = enrichedCompletedDF.filter(col("ministry").isin(stateList: _*) || col("mdo_name").isin(stateList: _*)).agg(countDistinct("certificate_id").as("content_state_completions")).first().getLong(0)
      val centralCompletedCount = totalCompletedCount - stateCompletedCount
      println("state completions : "+ stateCompletedCount)
      println("central completions : "+ centralCompletedCount)
      Redis.update("dashboard_state_content_completions", stateCompletedCount.toString)
      Redis.update("dashboard_central_content_completions", centralCompletedCount.toString)



      val enrichedEventEnrolmentsDF = eventsEnrolmentDataDF.join(userWithOrgDF, Seq("user_id"), "inner")
      val totalEventEnrolments = enrichedEventEnrolmentsDF.count()
      val stateEventEnrolmentsCount = enrichedEventEnrolmentsDF.filter(col("ministry").isin(stateList: _*) || col("mdo_name").isin(stateList: _*)).count()
      val centralEventEnrolmentsCount = totalEventEnrolments - stateEventEnrolmentsCount
      println("state event enrolments : "+ stateEventEnrolmentsCount)
      println("central event enrolments : "+ centralEventEnrolmentsCount)
      Redis.update("dashboard_state_event_enrolments", stateEventEnrolmentsCount.toString)
      Redis.update("dashboard_central_event_enrolments", centralEventEnrolmentsCount.toString)



      val enrichedEventCompletionsDF = eventsEnrolmentDataDF.filter(col("certificate_id").isNotNull).join(userWithOrgDF, Seq("user_id"), "inner")
      val totalEventCompletions = enrichedEventCompletionsDF.count()
      val stateEventCompletionCount = enrichedEventEnrolmentsDF.filter(col("ministry").isin(stateList: _*) || col("mdo_name").isin(stateList: _*)).agg(countDistinct("certificate_id").as("event_state_completions")).first().getLong(0)
      val centralEventCompletionCount = totalEventCompletions - stateEventCompletionCount
      println("state event completions : "+ stateEventCompletionCount)
      println("central event completions : "+ centralEventCompletionCount)
      Redis.update("dashboard_state_event_completions", stateEventCompletionCount.toString)
      Redis.update("dashboard_central_event_completions", centralEventCompletionCount.toString)


      val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
      val zoneOffset = ZoneOffset.ofHoursMinutes(5, 30)
      val currentDate = LocalDate.now()
      val previousDayStart = currentDate.minusDays(1).atStartOfDay().atOffset(zoneOffset)
      val previousDayEnd = currentDate.atStartOfDay().minusSeconds(1).atOffset(zoneOffset)
      val previousStart = previousDayStart.format(dateTimeFormatter)
      val previousEnd = previousDayEnd.format(dateTimeFormatter)

      println("previous day start : "+ previousStart)
      println("previous day end : "+ previousEnd)

      val totalCertsCount = enrichedEnrolmentsDF.filter(col("certificate_id").isNotNull)
        .filter(col("first_certificate_generated_on") >= previousStart && col("first_certificate_generated_on") <= previousEnd).agg(countDistinct("certificate_id").as("total_certificates"))
        .first().getLong(0)

      val stateCertsCount = enrichedEnrolmentsDF.filter(col("certificate_id").isNotNull)
        .filter(col("first_certificate_generated_on") >= previousStart && col("first_certificate_generated_on") <= previousEnd)
        .filter(col("ministry").isin(stateList: _*) || col("mdo_name").isin(stateList: _*)).agg(countDistinct("certificate_id").as("state_certificates"))
        .first().getLong(0)

      val centralCertsCount = totalCertsCount - stateCertsCount


      val totalEventCertsCount = enrichedEventEnrolmentsDF.filter(col("certificate_id").isNotNull)
        .filter(col("completed_on_datetime") >= previousStart && col("completed_on_datetime") <= previousEnd)
        .agg(countDistinct("certificate_id").as("total_event_certificates")).first().getLong(0)

      val stateEventCertsCount = enrichedEventEnrolmentsDF.filter(col("certificate_id").isNotNull)
        .filter(col("completed_on_datetime") >= previousStart && col("completed_on_datetime") <= previousEnd)
        .filter(col("ministry").isin(stateList: _*) || col("mdo_name").isin(stateList: _*)).agg(countDistinct("certificate_id").as("state_event_certificates"))
        .first().getLong(0)

      val centralEventCertsCount = totalEventCertsCount - stateEventCertsCount

      val stateCertificatesIssuedYday = stateCertsCount + stateEventCertsCount
      val centralCertificatesIssuedYday = centralCertsCount + centralEventCertsCount
      println("total certificate generated yday state : "+ stateCertificatesIssuedYday)
      println("total certificate generated yday centre : "+ centralCertificatesIssuedYday)
      Redis.update("dashboard_state_certificate_generated_yday", stateCertificatesIssuedYday.toString)
      Redis.update("dashboard_central_certificate_generated_yday", centralCertificatesIssuedYday.toString)


      val stateUserCount = activeUsersDF.filter(col("ministry").isin(stateList: _*) || col("mdo_name").isin(stateList: _*)).agg(count("user_id").as("state_user_count")).first().getLong(0)
      val totalUserCount = activeUsersDF.count()
      val centralUserCount = totalUserCount - stateUserCount
      println("total users registered state : "+ stateUserCount)
      println("total users registered centre : "+ centralUserCount)
      Redis.update("dashboard_state_registered_users", stateUserCount.toString)
      Redis.update("dashboard_central_registered_users", centralUserCount.toString)


      val filteredUserRegisteredYdayDF = activeUsersDF.filter(col("user_registration_date") > previousStart && col("user_registration_date") < previousEnd)
      val totalUsersRegisteredYdayCount = filteredUserRegisteredYdayDF.count()
      val stateUserRegisteredYdayCount = filteredUserRegisteredYdayDF.filter(col("ministry").isin(stateList: _*) || col("mdo_name").isin(stateList: _*)).agg(count("user_id").as("state_user_count")).first().getLong(0)
      val centralUserRegisteredYdayCount = totalUsersRegisteredYdayCount - stateUserRegisteredYdayCount
      println("total users registered yday state : "+ stateUserRegisteredYdayCount)
      println("total users registered yday centre : "+ centralUserRegisteredYdayCount)
      Redis.update("dashboard_state_registered_users_yday", stateUserRegisteredYdayCount.toString)
      Redis.update("dashboard_central_registered_users_yday", centralUserRegisteredYdayCount.toString)


      val query = raw"""SELECT DISTINCT(uid) as user_id FROM \"summary-events\" WHERE dimensions_type='app' AND __time > CURRENT_TIMESTAMP - INTERVAL '30' DAY"""
      val monthlyActiveUsersDF = druidDFOption(query, conf.sparkDruidRouterHost, limit = 1000000).getOrElse(emptySchemaDataFrame(loginSchema))
      val monthlyActiveUsersWithMdoDF = monthlyActiveUsersDF.join(userWithOrgDF, Seq("user_id"), "left").filter(col("user_id").isNotNull && col("mdo_id").isNotNull)
      val stateMonthlyActiveUserCount = monthlyActiveUsersWithMdoDF.filter(col("ministry").isin(stateList: _*) || col("mdo_name").isin(stateList: _*)).agg(countDistinct("user_id")).first().getLong(0)
      val centralMonthlyActiveUserCount = monthlyActiveUsersWithMdoDF.filter(!(col("ministry").isin(stateList: _*) || col("mdo_name").isin(stateList: _*))).agg(countDistinct("user_id")).first().getLong(0)
      println("monthly active users state : "+ stateMonthlyActiveUserCount)
      println("monthly active users centre : "+ centralMonthlyActiveUserCount)
      Redis.update("dashboard_state_monthly_active_users", stateMonthlyActiveUserCount.toString)
      Redis.update("dashboard_central_monthly_active_users", centralMonthlyActiveUserCount.toString)


      val loginYdayQuery = raw"""SELECT DISTINCT(actor_id) AS user_id FROM \"telemetry-events-syncts\" WHERE eid='IMPRESSION' AND actor_type='User' AND __time >= TIME_FLOOR(CURRENT_TIMESTAMP + INTERVAL '5:30' HOUR TO MINUTE - INTERVAL '24' HOUR, 'P1D') AND __time < TIME_FLOOR(CURRENT_TIMESTAMP + INTERVAL '5:30' HOUR TO MINUTE, 'P1D')""".stripMargin
      val loggedInUsersDF = druidDFOption(loginYdayQuery, conf.sparkDruidRouterHost, limit = 1000000).getOrElse(emptySchemaDataFrame(loginSchema)) // Fallback to empty if Druid returns nothing
      val loggedInWithMdoDF = loggedInUsersDF.join(userWithOrgDF, Seq("user_id"), "left").filter(col("user_id").isNotNull && col("mdo_id").isNotNull)
      val stateUserLoggedInYesterday = loggedInWithMdoDF.filter(col("ministry").isin(stateList: _*) || col("mdo_name").isin(stateList: _*)).agg(countDistinct("user_id")).first().getLong(0)
      val centralUserLoggedInYesterday = loggedInWithMdoDF.filter(!(col("ministry").isin(stateList: _*) || col("mdo_name").isin(stateList: _*))).agg(countDistinct("user_id")).first().getLong(0)
      println("users logged in yday state : "+ stateUserLoggedInYesterday)
      println("users logged in yday centre : "+ centralUserLoggedInYesterday)
      Redis.update("dashboard_state_users_logged_in_yday", stateUserLoggedInYesterday.toString)
      Redis.update("dashboard_central_users_logged_in_yday", centralUserLoggedInYesterday.toString)
      Redis.closeRedisConnect()
    } catch {
      case e: Exception =>
        println(s"Error occurred during DSRComputationModel processing: ${e.getMessage}", e)
        System.exit(1)
    }
  }
}
DSRComputationModel