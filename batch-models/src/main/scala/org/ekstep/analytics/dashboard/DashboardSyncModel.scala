package org.ekstep.analytics.dashboard

import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.framework._
import org.joda.time.DateTime
import org.apache.spark.sql.functions.col
import java.text.SimpleDateFormat
import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.time.LocalDate

/**
 * Model for processing dashboard data
 */
object DashboardSyncModel extends AbsDashboardModel {

  implicit val className: String = "org.ekstep.analytics.dashboard.DashboardSyncModel"
  override def name() = "DashboardSyncModel"

  /**
   * Master method, does all the work, fetching, processing and dispatching
   *
   * @param timestamp unique timestamp from the start of the processing
   */
  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {

    val processingTime = new SimpleDateFormat(s"${dateFormat}'T'${timeFormat}'Z'").format(timestamp)
    Redis.update("dashboard_update_time", processingTime)

    // obtain and save user org data
    val (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()
    val activeUsers = userDF.where(col("userStatus") === 1).cache()
    val activeOrgs = orgDF.where(col("orgStatus") === 1).cache()

    val designationsDF = orgDesignationsDF(userOrgDF)
    Redis.dispatchDataFrame[String]("org_designations", designationsDF, "userOrgID", "org_designations", replace = false)

    // kafkaDispatch(withTimestamp(orgDF, timestamp), conf.orgTopic)
    kafkaDispatch(withTimestamp(userOrgDF, timestamp), conf.userOrgTopic)

    // obtain and save role count data
    val roleDF = roleDataFrame()
    val userOrgRoleDF = userOrgRoleDataFrame(userOrgDF, roleDF).cache()
    val roleCountDF = roleCountDataFrame(userOrgRoleDF)
    kafkaDispatch(withTimestamp(roleCountDF, timestamp), conf.roleUserCountTopic)

    // obtain and save org role count data
    val orgRoleCount = orgRoleCountDataFrame(userOrgRoleDF)
    kafkaDispatch(withTimestamp(orgRoleCount, timestamp), conf.orgRoleUserCountTopic)

    // org user count
    val orgUserCountDF = orgUserCountDataFrame(activeOrgs, activeUsers)
    // validate activeOrgCount and orgUserCountDF count
    validate({orgUserCountDF.count()},
      {userOrgDF.filter(expr("userStatus=1 AND userOrgID IS NOT NULL AND userOrgStatus=1")).select("userOrgID").distinct().count()},
      "orgUserCountDF.count() should equal distinct active org count in userOrgDF")

    //obtain and save total karma points of each user
    val karmaPointsDataDF = cache.load("userKarmaPoints")
      .groupBy(col("userid").alias("userID")).agg(sum(col("points")).alias("total_points"))

    val kPointsWithUserOrgDF = karmaPointsDataDF.join(userOrgDF, Seq("userID"), "inner")
      .select(karmaPointsDataDF("*"), userOrgDF("fullName"), userOrgDF("userOrgID"), userOrgDF("userOrgName"), userOrgDF("professionalDetails.designation").alias("designation"), userOrgDF("userProfileImgUrl"))

    val (hierarchyDF, allCourseProgramDetailsWithCompDF, allCourseProgramDetailsDF,
    allCourseProgramDetailsWithRatingDF) = contentDataFrames(orgDF)

    kafkaDispatch(withTimestamp(allCourseProgramDetailsWithRatingDF, timestamp), conf.allCourseTopic)

    // get course competency mapping data, dispatch to kafka to be ingested by druid data-source: dashboards-course-competency
    val allCourseProgramCompetencyDF = allCourseProgramCompetencyDataFrame(allCourseProgramDetailsWithCompDF).cache()
    kafkaDispatch(withTimestamp(allCourseProgramCompetencyDF, timestamp), conf.courseCompetencyTopic)

    // get course completion data, dispatch to kafka to be ingested by druid data-source: dashboards-user-course-program-progress

    val userCourseProgramCompletionDF = userCourseProgramCompletionDataFrame(datesAsLong = true).cache()
    val allCourseProgramCompletionWithDetailsDF = allCourseProgramCompletionWithDetailsDataFrame(userCourseProgramCompletionDF, allCourseProgramDetailsDF, userOrgDF)

    validate({userCourseProgramCompletionDF.count()}, {allCourseProgramCompletionWithDetailsDF.count()}, "userCourseProgramCompletionDF.count() should equal final course progress DF count")
    kafkaDispatch(withTimestamp(allCourseProgramCompletionWithDetailsDF, timestamp), conf.userCourseProgramProgressTopic)

    // org user details redis dispatch
    val (orgRegisteredUserCountMap, orgTotalUserCountMap, orgNameMap) = getOrgUserMaps(orgUserCountDF)
    val activeOrgCount = activeOrgs.count()
    val activeUserCount = activeUsers.count()
    Redis.dispatch(conf.redisRegisteredOfficerCountKey, orgRegisteredUserCountMap)
    Redis.dispatch(conf.redisTotalOfficerCountKey, orgTotalUserCountMap)
    Redis.dispatch(conf.redisOrgNameKey, orgNameMap)
    Redis.update(conf.redisTotalRegisteredOfficerCountKey, activeUserCount.toString)
    Redis.update(conf.redisTotalOrgCountKey, activeOrgCount.toString)

    // update redis key for top 10 learners in MDO channel
    val toJsonStringUDF = udf((userID: String, fullName: String, userOrgName: String, designation: String, userProfileImgUrl: String, total_points: Long, rank: Int) => {
      s"""{"userID":"$userID","fullName":"$fullName","userOrgName":"$userOrgName","designation":"$designation","userProfileImgUrl":"$userProfileImgUrl","total_points":$total_points,"rank":$rank}"""
    })
    val windowSpec = Window.partitionBy("userOrgID").orderBy(col("total_points").desc)
    val rankedDF = kPointsWithUserOrgDF.withColumn("rank", rank().over(windowSpec))
    val top10LearnersByMDODF = rankedDF.filter(col("rank") <= 10)
    val jsonStringDF = top10LearnersByMDODF.withColumn("json_details", toJsonStringUDF(
      col("userID"), col("fullName"), col("userOrgName"), col("designation"), col("userProfileImgUrl"), col("total_points"), col("rank")
    )).groupBy("userOrgID").agg(collect_list(col("json_details")).as("top_learners"))
    val resultDF = jsonStringDF.select(col("userOrgID"), to_json(struct(col("top_learners"))).alias("top_learners"))

    Redis.dispatchDataFrame[String]("dashboard_top_10_learners_on_kp_by_user_org", resultDF, "userOrgID", "top_learners")

    val (hierarchy1DF, cbpDetailsWithCompDF, cbpDetailsDF,
    cbpDetailsWithRatingDF) = contentDataFrames(orgDF, Seq("Course", "Program", "Blended Program", "Curated Program"))
    val cbpCompletionWithDetailsDF = allCourseProgramCompletionWithDetailsDataFrame(userCourseProgramCompletionDF, cbpDetailsDF, userOrgDF)

    // update redis data for learner home page
    updateLearnerHomePageData(orgDF, userOrgDF, userCourseProgramCompletionDF, cbpCompletionWithDetailsDF, cbpDetailsWithRatingDF)

    // update redis data for dashboards
    dashboardRedisUpdates(orgRoleCount, activeUsers, allCourseProgramDetailsWithRatingDF, allCourseProgramCompletionWithDetailsDF, allCourseProgramCompetencyDF, cbpCompletionWithDetailsDF)

    // update cbp top 10 reviews
    cbpTop10Reviews(allCourseProgramDetailsWithRatingDF)

    Redis.closeRedisConnect()
  }

  def dashboardRedisUpdates(orgRoleCount: DataFrame, activeUsers: DataFrame, allCourseProgramDetailsWithRatingDF: DataFrame,
                            allCourseProgramCompletionWithDetailsDF: DataFrame, allCourseProgramCompetencyDF: DataFrame, cbpCompletionWithDetailsDF: DataFrame)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    import spark.implicits._
    // new redis updates - start
    // MDO onboarded, with atleast one MDO_ADMIN/MDO_LEADER
    val orgWithMdoAdminLeaderCount = orgRoleCount.where(expr("role IN ('MDO_ADMIN', 'MDO_LEADER') AND count > 0")).select("orgID").distinct().count()
    val orgWithMdoAdminCount = orgRoleCount.where(expr("role IN ('MDO_ADMIN') AND count > 0")).select("orgID").distinct().count()
    Redis.update("dashboard_org_with_mdo_admin_leader_count", orgWithMdoAdminLeaderCount.toString)
    Redis.update("dashboard_org_with_mdo_admin_count", orgWithMdoAdminCount.toString)

    // mdo-wise registered user count
    val activeUsersByMDODF = activeUsers.groupBy("userOrgID").agg(count("*").alias("count"))
    Redis.dispatchDataFrame[Long]("dashboard_user_count_by_user_org", activeUsersByMDODF, "userOrgID", "count")

    // new users registered yesterday
    val usersRegisteredYesterdayDF = activeUsers
      .withColumn("yesterdayStartTimestamp", date_trunc("day", date_sub(current_timestamp(), 1)).cast("long"))
      .withColumn("todayStartTimestamp", date_trunc("day", current_timestamp()).cast("long"))
    val usersRegisteredYesterdayCount = usersRegisteredYesterdayDF
      .where(expr("userCreatedTimestamp >= yesterdayStartTimestamp AND userCreatedTimestamp < todayStartTimestamp"))
      .count()
    Redis.update("dashboard_new_users_registered_yesterday", usersRegisteredYesterdayCount.toString)

    // cbp-wise live/draft/review/retired/pending-publish course counts
    val allCourseDF = allCourseProgramDetailsWithRatingDF.where(expr("category='Course'"))
    val allCourseModeratedCourseDF = allCourseProgramDetailsWithRatingDF.where(expr("category IN ('Course', 'Moderated Course')"))
    val liveCourseDF = allCourseDF.where(expr("courseStatus='Live'"))
    val liveCourseModeratedCourseDF = allCourseModeratedCourseDF.where(expr("courseStatus='Live'"))
    val draftCourseDF = allCourseDF.where(expr("courseStatus='Draft'"))
    val reviewCourseDF = allCourseDF.where(expr("courseStatus='Review'"))
    val retiredCourseDF = allCourseDF.where(expr("courseStatus='Retired'"))
    val pendingPublishCourseDF = reviewCourseDF.where(expr("courseReviewStatus='Reviewed'"))
    val liveContentDF = allCourseProgramDetailsWithRatingDF.where(expr("courseStatus='Live'"))

    val liveCourseCountByCBPDF = liveCourseDF.groupBy("courseOrgID").agg(count("*").alias("count"))
    Redis.dispatchDataFrame[Long]("dashboard_live_course_count_by_course_org", liveCourseCountByCBPDF, "courseOrgID", "count")
    val liveCourseModeratedCourseByCBPDF = liveCourseModeratedCourseDF.groupBy("courseOrgID").agg(count("*").alias("count"))
    Redis.dispatchDataFrame[Long]("dashboard_live_course_moderated_course_count_by_course_org", liveCourseModeratedCourseByCBPDF, "courseOrgID", "count")
    val liveContentCountByCBPDF = liveContentDF.groupBy("courseOrgID").agg(count("*").alias("count"))
    Redis.dispatchDataFrame[Long]("dashboard_live_content_count_by_course_org", liveContentCountByCBPDF, "courseOrgID", "count")
    val draftCourseCountByCBPDF = draftCourseDF.groupBy("courseOrgID").agg(count("*").alias("count"))
    Redis.dispatchDataFrame[Long]("dashboard_draft_course_count_by_course_org", draftCourseCountByCBPDF, "courseOrgID", "count")
    val reviewCourseCountByCBPDF = reviewCourseDF.groupBy("courseOrgID").agg(count("*").alias("count"))
    Redis.dispatchDataFrame[Long]("dashboard_review_course_count_by_course_org", reviewCourseCountByCBPDF, "courseOrgID", "count")
    val retiredCourseCountByCBPDF = retiredCourseDF.groupBy("courseOrgID").agg(count("*").alias("count"))
    Redis.dispatchDataFrame[Long]("dashboard_retired_course_count_by_course_org", retiredCourseCountByCBPDF, "courseOrgID", "count")
    val pendingPublishCourseCountByCBPDF = pendingPublishCourseDF.groupBy("courseOrgID").agg(count("*").alias("count"))
    Redis.dispatchDataFrame[Long]("dashboard_pending_publish_course_count_by_course_org", pendingPublishCourseCountByCBPDF, "courseOrgID", "count")

    // MDO with at least one live course
    val orgWithLiveCourseCount = liveCourseDF.select("courseOrgID").distinct().count()
    Redis.update("dashboard_cbp_with_live_course_count", orgWithLiveCourseCount.toString)

    // Average rating across all live courses with ratings, and by CBP
    val ratedLiveCourseDF = liveCourseDF.where(col("ratingAverage").isNotNull)
    val ratedLiveCourseModeratedCourseDF = liveCourseModeratedCourseDF.where(col("ratingAverage").isNotNull)
    val avgRatingOverall = ratedLiveCourseDF.agg(avg("ratingAverage").alias("ratingAverage")).select("ratingAverage").first().getDouble(0)
    Redis.update("dashboard_course_average_rating_overall", avgRatingOverall.toString)

    val avgRatingByCBPDF = ratedLiveCourseDF.groupBy("courseOrgID").agg(avg("ratingAverage").alias("ratingAverage"))
    Redis.dispatchDataFrame[Double]("dashboard_course_average_rating_by_course_org", avgRatingByCBPDF, "courseOrgID", "ratingAverage")

    val courseModeratedCourseAvgRatingByCBPDF = ratedLiveCourseModeratedCourseDF.groupBy("courseOrgID").agg(avg("ratingAverage").alias("ratingAverage"))
    Redis.dispatchDataFrame[Double]("dashboard_course_moderated_course_average_rating_by_course_org", courseModeratedCourseAvgRatingByCBPDF, "courseOrgID", "ratingAverage")

    // cbpwise content average rating
    val ratedLiveContentDF = liveContentDF.where(col("ratingAverage").isNotNull)
    val avgContentRatingByCBPDF = ratedLiveContentDF.groupBy("courseOrgID").agg(avg("ratingAverage").alias("ratingAverage"))
    Redis.dispatchDataFrame[Double]("dashboard_content_average_rating_by_course_org", avgContentRatingByCBPDF, "courseOrgID", "ratingAverage")

    // enrollment/not-started/started/in-progress/completion count, live and retired courses
    val liveRetiredContentEnrolmentDF = allCourseProgramCompletionWithDetailsDF.where(expr("courseStatus IN ('Live', 'Retired') AND userStatus=1"))
    val liveRetiredCourseEnrolmentDF = allCourseProgramCompletionWithDetailsDF.where(expr("category='Course' AND courseStatus IN ('Live', 'Retired') AND userOrgID IS NOT NULL")).cache()
    val liveRetiredCourseProgramEnrolmentDF = allCourseProgramCompletionWithDetailsDF.where(expr("category IN ('Course', 'Program') AND courseStatus IN ('Live', 'Retired') AND userStatus=1"))
    val liveRetiredCourseProgramExcludingModeratedEnrolmentDF = allCourseProgramCompletionWithDetailsDF.where(expr("category IN ('Course', 'Program', 'Blended Program', 'CuratedCollections', 'Standalone Assessment', 'Curated Program') AND courseStatus IN ('Live', 'Retired') AND userStatus=1"))
    val liveRetiredCourseModeratedCourseEnrolmentDF = allCourseProgramCompletionWithDetailsDF.where(expr("category IN ('Course', 'Moderated Course') AND courseStatus IN ('Live', 'Retired') AND userStatus=1"))

    //get only Live counts
    val liveCourseProgramEnrolmentDF = liveRetiredCourseProgramEnrolmentDF.where(expr("courseStatus = 'Live'"))
    val liveCourseProgramExcludingModeratedEnrolmentDF = liveRetiredCourseProgramExcludingModeratedEnrolmentDF.where(expr("courseStatus = 'Live'"))
    val liveCourseModeratedCourseEnrolmentDF = liveRetiredCourseModeratedCourseEnrolmentDF.where(expr("courseStatus = 'Live'"))

    val currentDate = LocalDate.now()
    // Calculate twenty four hours ago
    val twentyFourHoursAgo = currentDate.minusDays(1)
    // Convert to LocalDateTime by adding a time component (midnight)
    val twentyFourHoursAgoLocalDateTime = twentyFourHoursAgo.atStartOfDay()
    // Get the epoch time in milliseconds with IST offset
    val twentyFourHoursAgoEpochMillis = twentyFourHoursAgoLocalDateTime.toEpochSecond(java.time.ZoneOffset.ofHoursMinutes(5, 30))
    val liveRetiredCourseProgramCompletedYesterdayDF = allCourseProgramCompletionWithDetailsDF.where(expr(s"category IN ('Course', 'Program') AND courseStatus IN ('Live', 'Retired') AND userStatus=1 AND dbCompletionStatus=2 AND courseCompletedTimestamp >= ${twentyFourHoursAgoEpochMillis}"))
    // Calculate twelve months ago
    val twelveMonthsAgo = currentDate.minusMonths(12)
    // Convert to LocalDateTime by adding a time component (midnight)
    val twelveMonthsAgoLocalDateTime = twelveMonthsAgo.atStartOfDay()
    // Get the epoch time in milliseconds with IST offset
    val twelveMonthsAgoEpochMillis = twelveMonthsAgoLocalDateTime.toEpochSecond(java.time.ZoneOffset.ofHoursMinutes(5, 30))
    println("=====================")
    println(twelveMonthsAgoEpochMillis)
    println("======================================")
    val liveRetiredCourseEnrolmentsInLast12MonthsDF = allCourseProgramCompletionWithDetailsDF.where(expr(s"category='Course' AND courseStatus IN ('Live', 'Retired') AND userStatus=1 AND courseEnrolledTimestamp >= ${twelveMonthsAgoEpochMillis}"))
    // started + not-started = enrolled
    val liveRetiredCourseNotStartedDF = liveRetiredCourseEnrolmentDF.where(expr("dbCompletionStatus=0"))
    val liveRetiredCourseStartedDF = liveRetiredCourseEnrolmentDF.where(expr("dbCompletionStatus IN (1, 2)"))
    // in-progress + completed = started
    val liveRetiredCourseInProgressDF = liveRetiredCourseStartedDF.where(expr("dbCompletionStatus=1"))
    val liveRetiredCourseCompletedDF = liveRetiredCourseStartedDF.where(expr("dbCompletionStatus=2"))
    val liveRetiredContentCompletedDF = liveRetiredContentEnrolmentDF.where(expr("dbCompletionStatus=2"))
    val liveRetiredCourseEnrolmentsCompletionsDF = liveRetiredCourseStartedDF.where(expr("dbCompletionStatus IN (0, 1, 2)"))
    // course program completed
    val liveRetiredCourseProgramCompletedDF = liveRetiredCourseProgramEnrolmentDF.where(expr("dbCompletionStatus=2"))
    val liveCourseProgramExcludingModeratedCompletedDF= liveCourseProgramExcludingModeratedEnrolmentDF.where(expr("dbCompletionStatus=2")).cache()

    // do both count(*) and countDistinct(userID) aggregates at once
    val enrolmentCountDF = liveRetiredCourseEnrolmentDF.agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    val notStartedCountDF = liveRetiredCourseNotStartedDF.agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    val startedCountDF = liveRetiredCourseStartedDF.agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    val inProgressCountDF = liveRetiredCourseInProgressDF.agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    val completedCountDF = liveRetiredCourseCompletedDF.agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    val landingPageCompletedCountDF = liveRetiredCourseProgramCompletedDF.agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    val landingPageCompletedYesterdayCountDF = liveRetiredCourseProgramCompletedYesterdayDF.agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))

    // group by courseID to get enrolment counts of each course/program
    val liveCourseProgramEnrolmentCountsDF = liveCourseProgramEnrolmentDF.groupBy("courseID").agg(count("*").alias("enrolmentCount"))
    // unique user counts
    val enrolmentUniqueUserCount = enrolmentCountDF.select("uniqueUserCount").first().getLong(0)
    val notStartedUniqueUserCount = notStartedCountDF.select("uniqueUserCount").first().getLong(0)
    val startedUniqueUserCount = startedCountDF.select("uniqueUserCount").first().getLong(0)
    val inProgressUniqueUserCount = inProgressCountDF.select("uniqueUserCount").first().getLong(0)
    val completedUniqueUserCount = completedCountDF.select("uniqueUserCount").first().getLong(0)

    Redis.update("dashboard_unique_users_enrolled_count", enrolmentUniqueUserCount.toString)
    Redis.update("dashboard_unique_users_not_started_count", notStartedUniqueUserCount.toString)
    Redis.update("dashboard_unique_users_started_count", startedUniqueUserCount.toString)
    Redis.update("dashboard_unique_users_in_progress_count", inProgressUniqueUserCount.toString)
    Redis.update("dashboard_unique_users_completed_count", completedUniqueUserCount.toString)

    // counts
    val enrolmentCount = enrolmentCountDF.select("count").first().getLong(0)
    val notStartedCount = notStartedCountDF.select("count").first().getLong(0)
    val startedCount = startedCountDF.select("count").first().getLong(0)
    val inProgressCount = inProgressCountDF.select("count").first().getLong(0)
    val completedCount = completedCountDF.select("count").first().getLong(0)
    val landingPageCompletedCount = landingPageCompletedCountDF.select("count").first().getLong(0)
    val landingPageCompletedYesterdayCount = landingPageCompletedYesterdayCountDF.select("count").first().getLong(0)


    Redis.update("dashboard_enrolment_count", enrolmentCount.toString)
    Redis.update("dashboard_not_started_count", notStartedCount.toString)
    Redis.update("dashboard_started_count", startedCount.toString)
    Redis.update("dashboard_in_progress_count", inProgressCount.toString)
    Redis.update("dashboard_completed_count", completedCount.toString)
    Redis.update("lp_completed_count", landingPageCompletedCount.toString)
    Redis.update("lp_completed_yesterday_count", landingPageCompletedYesterdayCount.toString)
    Redis.dispatchDataFrame[Long]("live_course_program_enrolment_count", liveCourseProgramEnrolmentCountsDF, "courseID", "enrolmentCount")


    // mdo-wise enrollment/not-started/started/in-progress/completion counts
    val liveRetiredCourseEnrolmentByMDODF = liveRetiredCourseEnrolmentDF.groupBy("userOrgID").agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    val liveRetiredContentEnrolmentByMDODF = liveRetiredContentEnrolmentDF.groupBy("userOrgID").agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    val liveRetiredCourseEnrolmentsInLast12MonthsByMDODF = liveRetiredCourseEnrolmentsInLast12MonthsDF.groupBy("userOrgID").agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    Redis.dispatchDataFrame[Long]("dashboard_enrolment_count_by_user_org", liveRetiredCourseEnrolmentByMDODF, "userOrgID", "count")
    Redis.dispatchDataFrame[Long]("dashboard_enrolment_content_by_user_org", liveRetiredContentEnrolmentByMDODF, "userOrgID", "count")
    Redis.dispatchDataFrame[Long]("dashboard_enrolment_unique_user_count_by_user_org", liveRetiredCourseEnrolmentByMDODF, "userOrgID", "uniqueUserCount")
    Redis.dispatchDataFrame[Long]("dashboard_active_users_last_12_months_by_org", liveRetiredCourseEnrolmentsInLast12MonthsByMDODF, "userOrgID", "uniqueUserCount")
    val liveRetiredCourseNotStartedByMDODF = liveRetiredCourseNotStartedDF.groupBy("userOrgID").agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    Redis.dispatchDataFrame[Long]("dashboard_not_started_count_by_user_org", liveRetiredCourseNotStartedByMDODF, "userOrgID", "count")
    Redis.dispatchDataFrame[Long]("dashboard_not_started_unique_user_count_by_user_org", liveRetiredCourseNotStartedByMDODF, "userOrgID", "uniqueUserCount")
    val liveRetiredCourseStartedByMDODF = liveRetiredCourseStartedDF.groupBy("userOrgID").agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    Redis.dispatchDataFrame[Long]("dashboard_started_count_by_user_org", liveRetiredCourseStartedByMDODF, "userOrgID", "count")
    Redis.dispatchDataFrame[Long]("dashboard_started_unique_user_count_by_user_org", liveRetiredCourseStartedByMDODF, "userOrgID", "uniqueUserCount")
    val liveRetiredCourseInProgressByMDODF = liveRetiredCourseInProgressDF.groupBy("userOrgID").agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    Redis.dispatchDataFrame[Long]("dashboard_in_progress_count_by_user_org", liveRetiredCourseInProgressByMDODF, "userOrgID", "count")
    Redis.dispatchDataFrame[Long]("dashboard_in_progress_unique_user_count_by_user_org", liveRetiredCourseInProgressByMDODF, "userOrgID", "uniqueUserCount")
    val liveRetiredCourseCompletedByMDODF = liveRetiredCourseCompletedDF.groupBy("userOrgID").agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    Redis.dispatchDataFrame[Long]("dashboard_completed_count_by_user_org", liveRetiredCourseCompletedByMDODF, "userOrgID", "count")
    Redis.dispatchDataFrame[Long]("dashboard_completed_unique_user_count_by_user_org", liveRetiredCourseCompletedByMDODF, "userOrgID", "uniqueUserCount")


    // cbp-wise enrollment/not-started/started/in-progress/completion counts
    val liveRetiredCourseEnrolmentByCBPDF = liveRetiredCourseEnrolmentDF.groupBy("courseOrgID").agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    val liveRetiredContentCompletedByCBPDF = liveRetiredContentCompletedDF.groupBy("courseOrgID").agg(count("*").alias("count"))
    val liveRetiredCourseModeratedCourseEnrolmentByCBPDF = liveRetiredCourseModeratedCourseEnrolmentDF.groupBy("courseOrgID").agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    val liveRetiredContentEnrolmentByCBPDF = liveRetiredContentEnrolmentDF.groupBy("courseOrgID").agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    Redis.dispatchDataFrame[Long]("dashboard_content_completed_count_by_course_org", liveRetiredContentCompletedByCBPDF, "courseOrgID", "count")
    Redis.dispatchDataFrame[Long]("dashboard_enrolment_count_by_course_org", liveRetiredCourseEnrolmentByCBPDF, "courseOrgID", "count")
    Redis.dispatchDataFrame[Long]("dashboard_course_moderated_course_enrolment_count_by_course_org", liveRetiredCourseModeratedCourseEnrolmentByCBPDF, "courseOrgID", "count")
    Redis.dispatchDataFrame[Long]("dashboard_enrolment_content_by_course_org", liveRetiredContentEnrolmentByCBPDF, "courseOrgID", "count")
    Redis.dispatchDataFrame[Long]("dashboard_enrolment_unique_user_count_by_course_org", liveRetiredCourseEnrolmentByCBPDF, "courseOrgID", "uniqueUserCount")
    val liveRetiredCourseNotStartedByCBPDF = liveRetiredCourseNotStartedDF.groupBy("courseOrgID").agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    Redis.dispatchDataFrame[Long]("dashboard_not_started_count_by_course_org", liveRetiredCourseNotStartedByCBPDF, "courseOrgID", "count")
    Redis.dispatchDataFrame[Long]("dashboard_not_started_unique_user_count_by_course_org", liveRetiredCourseNotStartedByCBPDF, "courseOrgID", "uniqueUserCount")
    val liveRetiredCourseStartedByCBPDF = liveRetiredCourseStartedDF.groupBy("courseOrgID").agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    Redis.dispatchDataFrame[Long]("dashboard_started_count_by_course_org", liveRetiredCourseStartedByCBPDF, "courseOrgID", "count")
    Redis.dispatchDataFrame[Long]("dashboard_started_unique_user_count_by_course_org", liveRetiredCourseStartedByCBPDF, "courseOrgID", "uniqueUserCount")
    val liveRetiredCourseInProgressByCBPDF = liveRetiredCourseInProgressDF.groupBy("courseOrgID").agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    Redis.dispatchDataFrame[Long]("dashboard_in_progress_count_by_course_org", liveRetiredCourseInProgressByCBPDF, "courseOrgID", "count")
    Redis.dispatchDataFrame[Long]("dashboard_in_progress_unique_user_count_by_course_org", liveRetiredCourseInProgressByCBPDF, "courseOrgID", "uniqueUserCount")
    val liveRetiredCourseCompletedByCBPDF = liveRetiredCourseCompletedDF.groupBy("courseOrgID").agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    Redis.dispatchDataFrame[Long]("dashboard_completed_count_by_course_org", liveRetiredCourseCompletedByCBPDF, "courseOrgID", "count")
    Redis.dispatchDataFrame[Long]("dashboard_completed_unique_user_count_by_course_org", liveRetiredCourseCompletedByCBPDF, "courseOrgID", "uniqueUserCount")


    // cbp wise total certificate generations, competencies and top 10 content by completions for ati cti web page
    val certificateGeneratedDF = liveRetiredContentEnrolmentDF.filter($"certificateGeneratedOn".isNotNull && $"certificateGeneratedOn" =!= "")
    val certificateGeneratedByCBPDF = certificateGeneratedDF.groupBy("courseOrgID").agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    Redis.dispatchDataFrame[Long]("dashboard_certificates_generated_count_by_course_org", certificateGeneratedByCBPDF, "courseOrgID", "count")

    val courseModeratedCourseCertificateGeneratedDF = liveRetiredCourseModeratedCourseEnrolmentDF.filter($"certificateGeneratedOn".isNotNull && $"certificateGeneratedOn" =!= "")
    val courseModeratedCourseCertificateGeneratedByCBPDF = courseModeratedCourseCertificateGeneratedDF.groupBy("courseOrgID").agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    Redis.dispatchDataFrame[Long]("dashboard_course_moderated_course_certificates_generated_count_by_course_org", courseModeratedCourseCertificateGeneratedByCBPDF, "courseOrgID", "count")

    val courseIDCountsByCBPDF = liveRetiredContentEnrolmentDF.groupBy("courseOrgID", "courseID").count()
    val sortedCourseIDCountsByCBPDF = courseIDCountsByCBPDF.orderBy(col("count").desc)
    val courseIDsByCBPConcatenatedDF = sortedCourseIDCountsByCBPDF.groupBy("courseOrgID").agg(concat_ws(",", collect_list("courseID")).alias("sortedCourseIDs"))
    val competencyCountByCBPDF = courseIDsByCBPConcatenatedDF.withColumnRenamed("courseOrgID", "courseOrgID").withColumnRenamed("sortedCourseIDs", "courseIDs")
    Redis.dispatchDataFrame[Long]("dashboard_competencies_count_by_course_org", competencyCountByCBPDF, "courseOrgID", "courseIDs")

    // national learning week metrics
    val nationalLearningWeekStartString = conf.nationalLearningWeekStart
    val nationalLearningWeekEndString = conf.nationalLearningWeekEnd
    val zoneOffset = ZoneOffset.ofHoursMinutes(5, 30)
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    // Parse the strings to LocalDateTime
    val nationalLearningWeekStartDateTime = LocalDateTime.parse(nationalLearningWeekStartString, formatter)
    val nationalLearningWeekEndDateTime = LocalDateTime.parse(nationalLearningWeekEndString, formatter)
    val nationalLearningWeekStartOffsetDateTime = nationalLearningWeekStartDateTime.atOffset(zoneOffset)
    val nationalLearningWeekEndOffsetDateTime = nationalLearningWeekEndDateTime.atOffset(zoneOffset)
    // Convert OffsetDateTime to epoch seconds
    val nationalLearningWeekStartDateTimeEpoch = nationalLearningWeekStartOffsetDateTime.toEpochSecond
    val nationalLearningWeekEndDateTimeEpoch = nationalLearningWeekEndOffsetDateTime.toEpochSecond

    val eventsEnrolmentDataDF = cache.load("eventEnrolmentDetails")
    /* total certificates issued yesterday across all types of content */
    val certificateDateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssXXX")
    val eventsDateTimeFormatter=DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

    val nationalLearningWeekStartDate = nationalLearningWeekStartOffsetDateTime.format(eventsDateTimeFormatter)
    val nationalLearningWeekEndDate = nationalLearningWeekEndOffsetDateTime.format(eventsDateTimeFormatter)
    println("nationalLearningWeekStartDate",nationalLearningWeekStartDate)
    println("nationalLearningWeekEndDate",nationalLearningWeekEndDate)

    // NLW events enrollment data Filter the DataFrame where enrolled_on_datetime is within NLW
    val eventEnrolledDF = eventsEnrolmentDataDF
      .filter(col("enrolled_on_datetime") >= nationalLearningWeekStartDate && col("enrolled_on_datetime") <= nationalLearningWeekEndDate)

    // Count the distinct certificate_ids
    val eventEnrolledCountDF = eventEnrolledDF
      .agg(count("event_id").alias("event_count"))

    // Retrieve the count
    val enrolmentEventNLWCount = eventEnrolledCountDF
      .select("event_count")
      .first()
      .getLong(0)

    /* total enrolments that week across all types of content */
    println(liveRetiredContentEnrolmentDF.count())
    val enrolmentContentNLWDF = liveRetiredContentEnrolmentDF.filter($"courseEnrolledTimestamp" >= nationalLearningWeekStartDateTimeEpoch && $"courseEnrolledTimestamp" <= nationalLearningWeekEndDateTimeEpoch)
    val enrolmentContentNLWCountDF = enrolmentContentNLWDF.agg(count("*").alias("count"))
    val enrolmentContentNLWCount = enrolmentContentNLWCountDF.select("count").first().getLong(0)

    val totalEnrollmentNLWCount= enrolmentEventNLWCount + enrolmentContentNLWCount
    Redis.update("dashboard_content_enrolment_nlw_count", totalEnrollmentNLWCount.toString)

    // Calculate start and end of the previous day as OffsetDateTime
    val previousDayStart = currentDate.minusDays(1).atStartOfDay().atOffset(zoneOffset)
    val previousDayEnd = currentDate.atStartOfDay().minusSeconds(1).atOffset(zoneOffset)
    val previousDayStartString = previousDayStart.format(certificateDateTimeFormatter)
    val previousDayEndString = previousDayEnd.format(certificateDateTimeFormatter)
    val certificateGeneratedYdayDF = liveRetiredContentEnrolmentDF.filter($"certificateGeneratedOn" >= previousDayStartString && $"certificateGeneratedOn" <= previousDayEndString)
    val certificateGeneratedYdayCountDF = certificateGeneratedYdayDF.agg(count("*").alias("count"))
    val certificateGeneratedYdayCount = certificateGeneratedYdayCountDF.select("count").first().getLong(0)
    val previousStart = previousDayStart.format(eventsDateTimeFormatter)
    val previousEnd = previousDayEnd.format(eventsDateTimeFormatter)

    //Redis.update("dashboard_content_certificates_generated_yday_nlw_count", certificateGeneratedYdayCount.toString)

    // NLW events data Filter the DataFrame where status is 'completed' and the enrolled_on_datetime is within yesterday's range
    val eventCertificatesGeneratedYdayDF = eventsEnrolmentDataDF
      .filter(col("status") === "completed")
      .filter(col("enrolled_on_datetime") >= previousStart && col("enrolled_on_datetime") <= previousEnd)
      .filter(col("certificate_id").isNotNull) // Ensuring the certificate_id exists

    // Count the distinct certificate_ids
    val eventCertificateGeneratedYdayCountDF = eventCertificatesGeneratedYdayDF
      .agg(countDistinct("certificate_id").alias("event_certificate_count"))

    // Retrieve the count
    val eventCertificateGeneratedYdayCount = eventCertificateGeneratedYdayCountDF
      .select("event_certificate_count")
      .first()
      .getLong(0)

    // dashboard_event_certificates_generated_yday_nlw_count contains event count
    Redis.update("dashboard_event_certificates_generated_yday_nlw_count", eventCertificateGeneratedYdayCount.toString)

    //Total number of certificated yeasterday both event+content
    val totalCertificatesGeneratedYdayCount = certificateGeneratedYdayCount + eventCertificateGeneratedYdayCount

   // dashboard_content_only_certificates_generated_yday_nlw_count contains content counts
    Redis.update("dashboard_content_only_certificates_generated_yday_nlw_count", certificateGeneratedYdayCount.toString)

    //dashboard_content_certificates_generated_yday_nlw_count contains content+event count
    Redis.update("dashboard_content_certificates_generated_yday_nlw_count", totalCertificatesGeneratedYdayCount.toString)

    println("dashboard_event_certificates_generated_yday_nlw_count",eventCertificateGeneratedYdayCount.toString)
    println("dashboard_content_only_certificates_generated_yday_nlw_count", certificateGeneratedYdayCount.toString)
    println("dashboard_content_certificates_generated_yday_nlw_count",totalCertificatesGeneratedYdayCount.toString)

    /* total certificates issued that week across all types of content */
    val nationalLearningWeekStartDateTimeString = nationalLearningWeekStartOffsetDateTime.format(certificateDateTimeFormatter)
    val nationalLearningWeekEndDateTimeString = nationalLearningWeekEndOffsetDateTime.format(certificateDateTimeFormatter)
    println("=================")
    println(nationalLearningWeekStartDateTimeString)

    val eventCertificatesGeneratedNLWDF = eventsEnrolmentDataDF
      .filter(col("status") === "completed")
      .filter(col("enrolled_on_datetime") >= nationalLearningWeekStartDate && col("enrolled_on_datetime") <= nationalLearningWeekEndDate)
      .filter(col("certificate_id").isNotNull) // Ensuring the certificate_id exists

    // Count the distinct certificate_ids
    val eventCertificateGeneratedNLWCountDF = eventCertificatesGeneratedNLWDF
      .agg(countDistinct("certificate_id").alias("event_certificate_count"))

    // Retrieve the count
    val eventCertificateGeneratedNLWCount = eventCertificateGeneratedNLWCountDF
      .select("event_certificate_count")
      .first()
      .getLong(0)

    val certificateGeneratedInNLWDF = liveRetiredContentEnrolmentDF.filter($"certificateGeneratedOn" >= nationalLearningWeekStartDateTimeString && $"certificateGeneratedOn" <= nationalLearningWeekEndDateTimeString)
    val certificateGeneratedInNLWCountDF = certificateGeneratedInNLWDF.agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    val certificateGeneratedInNLWCount = certificateGeneratedInNLWCountDF.select("count").first().getLong(0)
    val totalCertificatesIssuedInNLW = certificateGeneratedInNLWCount + eventCertificateGeneratedNLWCount
    Redis.update("dashboard_content_certificates_generated_nlw_count", totalCertificatesIssuedInNLW.toString)

    /* total number of events published that week */
    val primaryCategory = "Event"
    val shouldClause = s"""{"match":{"contentType":"${primaryCategory}"}}"""
    val fields = Seq("identifier", "name", "startDate", "createdFor")
    val arrayFields = Seq("createdFor")

    // Corrected the split method
    val startDate = nationalLearningWeekStartString.split(" ")(0)
    val endDate = nationalLearningWeekEndString.split(" ")(0)

    val dateConditions = s"""{"range": {"startDate": {"gte": "${startDate}", "lte": "${endDate}"}}}"""
    val fieldsClause = fields.map(f => s""""${f}"""").mkString(",")

    val events_query = s"""{"_source":[${fieldsClause}],"query":{"bool":{"should":[${shouldClause}],"filter":[${dateConditions}]}}}"""

    val eventsPublishedInNLWDF = elasticSearchDataFrame(conf.sparkElasticsearchConnectionHost, "compositesearch", events_query, fields, arrayFields)
    println(eventsPublishedInNLWDF.count())

    // Make sure this Redis call is valid in your context
    val eventsPublishedInNLWCount = eventsPublishedInNLWDF.count()
    Redis.update("dashboard_events_published_nlw_count", eventsPublishedInNLWCount.toString())

    /* certificates issued by user that week across all types of content*/
    val certificateGeneratedInNLWByUserDF = certificateGeneratedInNLWDF.groupBy("userID").agg(count("*").alias("count"))
    Redis.dispatchDataFrame[String]("dashboard_content_certificates_issued_nlw_by_user", certificateGeneratedInNLWByUserDF, "userID", "count")

    /* learning hours by user that week across all types of content*/
    println("This is the problem start")
    show(cbpCompletionWithDetailsDF.select(col("courseEnrolledTimestamp")))
    val enrolmentContentDurationNLWByUserDF = cbpCompletionWithDetailsDF.filter($"courseEnrolledTimestamp" >= nationalLearningWeekStartDateTimeEpoch && $"courseEnrolledTimestamp" <= nationalLearningWeekEndDateTimeEpoch && $"userID" =!= "").groupBy("userID").agg(sum(expr("(completionPercentage / 100) * courseDuration")).alias("totalLearningSeconds"))
      .withColumn("totalLearningHours", bround(col("totalLearningSeconds") / 3600, 2)).select("userID", "totalLearningHours")
    println("This is the problem end")
    Redis.dispatchDataFrame[String]("dashboard_content_learning_hours_nlw_by_user", enrolmentContentDurationNLWByUserDF, "userID", "totalLearningHours")
    // get the count for each courseID
    val topContentCountDF = liveCourseProgramExcludingModeratedCompletedDF.groupBy("courseID").agg(count("*").alias("count"))
    val liveCourseProgramExcludingModeratedCompletedWithCountDF = liveCourseProgramExcludingModeratedCompletedDF.join(topContentCountDF, "courseID")

    val topCoursesByCBPDF = liveCourseProgramExcludingModeratedCompletedDF
      .filter($"category" === "Course")
    val ecount1DF = topCoursesByCBPDF.groupBy("courseOrgID", "courseID")
      .agg(countDistinct("userID").as("user_enrolment_count"))
    val windowSpec1 = Window.partitionBy("courseOrgID").orderBy(col("user_enrolment_count").desc)
    val sortedDF1 = ecount1DF
      .groupBy("courseOrgID")
      .agg(
        collect_list("courseID").as("courseIDs"),
        first("user_enrolment_count").as("user_enrolment_count")
      )
      .withColumn("sorted_courseIDs", concat_ws(",", col("courseIDs")))
      .select(
        concat(col("courseOrgID"), lit(":courses")).alias("courseOrgID:content"),
        col("sorted_courseIDs")
      )

    // average NPS
    val npsQuery = raw"""SELECT ROUND(((SUM(CASE WHEN rating IN (9, 10) THEN 1 ELSE 0 END) - SUM(CASE WHEN rating IN (0, 1, 2, 3, 4, 5, 6) THEN 1 ELSE 0 END)) * 1.0) / COUNT(rating) * 100, 1) AS avgNps FROM \"nps-upgraded-users-data\" WHERE submitted = true"""
    var npsDF = druidDFOption(npsQuery, conf.sparkDruidRouterHost).orNull
    if (npsDF == null) {
      npsDF = Seq((0)).toDF("avgNps")
    }
    val avgNPS = npsDF.select("avgNps").first().getDouble(0)
    // Convert the Double to a String
    val avgNPSString = avgNPS.toString

    // Update Redis with the string representation of avgNPS
    Redis.update("dashboard_nps_across_platform", avgNPSString)

    // competency coverage
    val categories = Seq("Course", "Program", "Blended Program", "CuratedCollections", "Standalone Assessment", "Curated Program")
    val cbpDetails = allCourseProgramESDataFrame(categories)
      .where("courseStatus IN ('Live', 'Retired')")
      .select("courseID", "competencyAreaId", "competencyThemeId", "competencySubThemeId", "courseName", "courseOrgID")
    // explode area, theme and sub theme seperately
    val areaExploded = cbpDetails.select(col("courseID"), expr("posexplode_outer(competencyAreaId) as (pos, competency_area_id)")).repartition(col("courseID"))
    val themeExploded = cbpDetails.select(col("courseID"), expr("posexplode_outer(competencyThemeId) as (pos, competency_theme_id)")).repartition(col("courseID"))
    val subThemeExploded = cbpDetails.select(col("courseID"), expr("posexplode_outer(competencySubThemeId) as (pos, competency_sub_theme_id)")).repartition(col("courseID"))
    // Joining area, theme and subtheme based on position
    val competencyJoinedDF = areaExploded.join(themeExploded, Seq("courseID", "pos")).join(subThemeExploded, Seq("courseID", "pos"))
    // joining with cbpDetails for getting courses with no competencies mapped to it
    val competencyContentMappingDF = cbpDetails
      .join(competencyJoinedDF, Seq("courseID"), "left")
      .dropDuplicates(Seq("courseID", "competency_area_id", "competency_theme_id", "competency_sub_theme_id", "courseOrgID"))
    val contentMappingDF = competencyContentMappingDF.select(col("courseID").alias("course_id"), col("competency_area_id"), col("competency_theme_id"), col("competency_sub_theme_id"), col("courseOrgID"))
    val areaWiseCountsDF = contentMappingDF.groupBy("courseOrgID", "competency_area_id").agg(countDistinct("competency_theme_id").alias("area_count")).filter(expr("competency_area_id IS NOT NULL"))
    val totalCountDF = contentMappingDF.groupBy("courseOrgID").agg(coalesce(countDistinct("competency_theme_id"), lit(0)).alias("total_count"))
    // Create a mapping for competency_area_id to descriptive keys
    val mappedAreaWiseCountsDF = areaWiseCountsDF.withColumn("mapped_area_id", when(col("competency_area_id") === 56, "Functional")
      .when(col("competency_area_id") === 1, "Behavioural")
      .when(col("competency_area_id") === 145, "Domain")
      .otherwise(col("competency_area_id")))

    val resultDF = mappedAreaWiseCountsDF.join(totalCountDF, "courseOrgID").groupBy("courseOrgID").agg(
      struct(
        first("total_count").alias("total"),  // Get the total_count
        map_from_entries(collect_list(struct(
          col("mapped_area_id").alias("key"),
          col("area_count").alias("value")
        ))).alias("area_count_map") // Create the map from the new column
      ).alias("jsonData")
    )
    Redis.dispatchDataFrame[String]("dashboard_competency_coverage_by_org", resultDF, "courseOrgID", "jsonData")
    val topProgramsByCBPDF = liveCourseProgramExcludingModeratedCompletedDF
      .filter($"category" === "Program")
    val ecount2DF = topProgramsByCBPDF.groupBy("courseOrgID", "courseID").agg(countDistinct("userID").as("user_enrolment_count"))
    //  val windowSpec2 = Window.partitionBy("courseOrgID").orderBy(col("user_enrolment_count").desc)
    val sortedDF2 = ecount2DF
      .groupBy("courseOrgID")
      .agg(
        collect_list("courseID").as("courseIDs"),
        first("user_enrolment_count").as("user_enrolment_count")
      )
      .withColumn("sorted_courseIDs", concat_ws(",", col("courseIDs")))
      .select(
        concat(col("courseOrgID"), lit(":programs")).alias("courseOrgID:content"),
        col("sorted_courseIDs")
      )


    val topAssessmentsByCBPDF = liveCourseProgramExcludingModeratedCompletedDF
      .filter($"category" === "Standalone Assessment")
    val ecount3DF = topAssessmentsByCBPDF.groupBy("courseOrgID", "courseID")
      .agg(countDistinct("userID").as("user_enrolment_count"))
    //    val windowSpec3 = Window.partitionBy("courseOrgID").orderBy(col("user_enrolment_count").desc)
    val sortedDF3 = ecount3DF
      .groupBy("courseOrgID")
      .agg(
        collect_list("courseID").as("courseIDs"),
        first("user_enrolment_count").as("user_enrolment_count")
      )
      .withColumn("sorted_courseIDs", concat_ws(",", col("courseIDs")))
      .select(
        concat(col("courseOrgID"), lit(":assessments")).alias("courseOrgID:content"),
        col("sorted_courseIDs")
      )
    val combinedDFByCBP = sortedDF1.union(sortedDF2).union(sortedDF3)

    Redis.dispatchDataFrame[String]("dashboard_top_10_courses_by_completion_by_course_org", combinedDFByCBP, "courseOrgID:content", "sorted_courseIDs")

    // Anonymous Assessment KPIs START
    val courseIDs = conf.anonymousAssessmentLoggedInUserContentIDs.split(",").toSeq
    val anonymousAssessmentIDs = conf.anonymousAssessmentNonLoggedInUserAssessmentIDs.split(",").toSeq
    val loggedInEnrolmentData = cache.load("enrolment")
      .where(expr("active=true"))
      .select(col("status"), col("userid"), col("courseid"), col("issued_certificates"))
      .filter(col("courseid").isin(courseIDs: _*))

    val nonLoggedInUserConpletionWIthAssessmentData = cassandraTableAsDataFrame(conf.cassandraUserKeyspace,conf.cassandraPublicUserAssessmentDataTable)
      .select(col("userid"), col("status"), col("issued_certificates"), col("assessmentid"))
      .filter(col("assessmentid").isin(anonymousAssessmentIDs: _*))

    val nonLoggedInUserAccessCount = nonLoggedInUserAccessCountDataFrame().select(col("user_count")).first().getLong(0)
    val loggedInUserAccessCount = loggedInUserAccessCountDataFrame().select(col("user_count")).first().getLong(0)
    val loggedInUserEnrolmentsCount = loggedInEnrolmentData
      .filter(col("status").isin(0, 1, 2)).agg(count("userid").alias("distinct_user_count"))
      .select(col("distinct_user_count")).first().getLong(0)
    val loggedInUserCompletionWithCertificateCount = loggedInEnrolmentData
      .filter(col("status") === 2 && size(col("issued_certificates")) > 0).agg(count("userid").alias("distinct_user_count"))
      .select(col("distinct_user_count")).first().getLong(0)
    val nonLoggedInUserCompletionWithCertificateCount = nonLoggedInUserConpletionWIthAssessmentData
      .filter(col("status") === "SUBMITTED" && size(col("issued_certificates")) > 0).agg(count("userid").alias("distinct_user_count"))
      .select(col("distinct_user_count")).first().getLong(0)

    Redis.update("dashboards_lu_assessment_access_count", loggedInUserAccessCount.toString)
    Redis.update("dashboards_lu_assessment_enrolment_count", loggedInUserEnrolmentsCount.toString)
    Redis.update("dashboards_lu_assessment_certification_count", loggedInUserCompletionWithCertificateCount.toString)
    Redis.update("dashboards_nlu_anonymous_assessment_access_count", nonLoggedInUserAccessCount.toString)
    Redis.update("dashboards_nlu_anonymous_assessment_certification_count", nonLoggedInUserCompletionWithCertificateCount.toString)

    // Anonymous Assessment KPIs END

    // DSR new keys monthly active users and certificate issued yesterday
    val averageMonthlyActiveUsersCount = averageMonthlyActiveUsersDataFrame()
    Redis.update("dashboard_average_monthly_active_users_last_30_days", averageMonthlyActiveUsersCount.toString)

    val tmpDF = liveRetiredCourseModeratedCourseEnrolmentDF.filter($"certificateGeneratedOn".isNotNull && $"certificateGeneratedOn" =!= "").select(col("certificateGeneratedOn"))
    val twenteyFourHoursAgoLocalDateTimeString = (twentyFourHoursAgoLocalDateTime.toString)+":00+0000"
    val courseModeratedCourseCertificateGeneratedYesterdayDF = liveRetiredCourseModeratedCourseEnrolmentDF.filter($"certificateGeneratedOn".isNotNull && $"certificateGeneratedOn" =!= "" && $"certificateGeneratedOn" >= twenteyFourHoursAgoLocalDateTimeString)
    val courseModeratedCourseCertificateGeneratedYesterdayCountDF = courseModeratedCourseCertificateGeneratedYesterdayDF.agg(count("*").alias("count"))
    val courseModeratedCourseCertificateGeneratedYesterdayCount = courseModeratedCourseCertificateGeneratedYesterdayCountDF.select("count").first().getLong(0)
    Redis.update("dashboard_course_moderated_course_certificates_generated_yesterday_count", courseModeratedCourseCertificateGeneratedYesterdayCount.toString)


    // courses enrolled/completed at-least once, only live courses
    val liveCourseEnrolmentDF = liveRetiredCourseEnrolmentDF.where(expr("courseStatus='Live'"))
    val liveCourseCompletedDF = liveRetiredCourseCompletedDF.where(expr("courseStatus='Live'"))

    val coursesEnrolledInDF = liveCourseEnrolmentDF.select("courseID").distinct()
    val coursesCompletedDF = liveCourseCompletedDF.select("courseID").distinct()

    val coursesEnrolledInIdList = coursesEnrolledInDF.map(_.getString(0)).filter(_.nonEmpty).collectAsList().toArray
    val coursesCompletedIdList = coursesCompletedDF.map(_.getString(0)).filter(_.nonEmpty).collectAsList().toArray

    val coursesEnrolledInCount = coursesEnrolledInIdList.length
    val coursesCompletedCount = coursesCompletedIdList.length

    Redis.update("dashboard_courses_enrolled_in_at_least_once", coursesEnrolledInCount.toString)
    Redis.update("dashboard_courses_completed_at_least_once", coursesCompletedCount.toString)
    Redis.update("dashboard_courses_enrolled_in_at_least_once_id_list", coursesEnrolledInIdList.mkString(","))
    Redis.update("dashboard_courses_completed_at_least_once_id_list", coursesCompletedIdList.mkString(","))

    // mdo-wise courses completed at-least once
    val liveCourseCompletedAtLeastOnceByMDODF = liveCourseCompletedDF.groupBy("userOrgID").agg(countDistinct("courseID").alias("count"))
    Redis.dispatchDataFrame[Long]("dashboard_courses_completed_at_least_once_by_user_org", liveCourseCompletedAtLeastOnceByMDODF, "userOrgID", "count")

    //mdo-wise 24 hour login percentage and certificates acquired
    val query = """SELECT dimension_channel AS userOrgID, COUNT(DISTINCT(uid)) as activeCount FROM \"summary-events\" WHERE dimensions_type='app' AND __time > CURRENT_TIMESTAMP - INTERVAL '24' HOUR GROUP BY 1"""
    var usersLoggedInLast24HrsByMDODF = druidDFOption(query, conf.sparkDruidRouterHost).orNull
    if (usersLoggedInLast24HrsByMDODF == null)
    {
      print("Empty dataframe: usersLoggedInLast24HrsByMDODF")
    }
    else
    {
      usersLoggedInLast24HrsByMDODF = usersLoggedInLast24HrsByMDODF.withColumn("activeCount", expr("CAST(activeCount as LONG)"))
      val loginPercentDF = usersLoggedInLast24HrsByMDODF.join(activeUsersByMDODF, Seq("userOrgID"))
      // Calculating login percentage
      val loginPercentbyMDODF = loginPercentDF.withColumn("loginPercentage", ((col("activeCount") / col("count")) * 100))
      // Selecting required columns
      val loginPercentLast24HrsbyMDODF = loginPercentbyMDODF.select("userOrgID", "loginPercentage")
      Redis.dispatchDataFrame[Long]("dashboard_login_percent_last_24_hrs_by_user_org", loginPercentLast24HrsbyMDODF, "userOrgID", "loginPercentage")
    }
    val certificateGeneratedByMDODF = certificateGeneratedDF.groupBy("userOrgID").agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    Redis.dispatchDataFrame[Long]("dashboard_certificates_generated_count_by_user_org", certificateGeneratedByMDODF, "userOrgID", "count")

    //mdo wise core competencies (providing the courses)
    val courseIDCountsByMDODF = liveRetiredContentEnrolmentDF.groupBy("userOrgID", "courseID").count()
    val sortedCourseIDCountsByMDODF = courseIDCountsByMDODF.orderBy(col("count").desc)
    val courseIDsByMDOConcatenatedDF = sortedCourseIDCountsByMDODF.groupBy("userOrgID").agg(concat_ws(",", collect_list("courseID")).alias("sortedCourseIDs"))
    val competencyCountByMDODF = courseIDsByMDOConcatenatedDF.withColumnRenamed("userOrgID", "userOrgID").withColumnRenamed("sortedCourseIDs", "courseIDs")
    Redis.dispatchDataFrame[Long]("dashboard_core_competencies_by_user_org", competencyCountByMDODF, "userOrgID", "courseIDs")


    // Top 5 Users - By course completion
    // SELECT userID, CONCAT(firstName, ' ', lastName) AS name, maskedEmail, COUNT(courseID) AS completed_count
    // FROM \"dashboards-user-course-program-progress\" WHERE __time = (SELECT MAX(__time) FROM \"dashboards-user-course-program-progress\")
    // AND userStatus=1 AND category='Course' AND courseStatus IN ('Live', 'Retired') AND dbCompletionStatus=2 $mdo$ GROUP BY 1, 2, 3 ORDER BY completed_count DESC LIMIT 5
    val top5UsersByCompletionByMdoDF = liveRetiredCourseCompletedDF
      .groupBy("userID", "fullName", "maskedEmail", "userOrgID", "userOrgName")
      .agg(count("courseID").alias("completedCount"))
      .groupByLimit(Seq("userOrgID"), "completedCount", 5, desc = true)
      .withColumn("jsonData", struct("rowNum", "userID", "fullName", "maskedEmail", "userOrgID", "userOrgName", "completedCount"))
      .orderBy(col("completedCount").desc)
      .groupBy("userOrgID")
      .agg(to_json(collect_list("jsonData")).alias("jsonData"))
    Redis.dispatchDataFrame[String]("dashboard_top_5_users_by_completion_by_org", top5UsersByCompletionByMdoDF, "userOrgID", "jsonData")

    // Top 5 Courses - By completion
    // SELECT courseID, courseName, category, courseOrgName, COUNT(userID) AS enrolled_count,
    // SUM(CASE WHEN dbCompletionStatus=0 THEN 1 ELSE 0 END) AS not_started_count,
    // SUM(CASE WHEN dbCompletionStatus=1 THEN 1 ELSE 0 END) AS in_progress_count,
    // SUM(CASE WHEN dbCompletionStatus=2 THEN 1 ELSE 0 END) AS \"Course Completions\"
    // FROM \"dashboards-user-course-program-progress\"
    // WHERE __time = (SELECT MAX(__time) FROM \"dashboards-user-course-program-progress\")
    // AND userStatus=1 AND category='Course' AND courseStatus IN ('Live', 'Retired') $mdo$
    // GROUP BY 1, 2, 3, 4 ORDER BY \"Course Completions\" DESC LIMIT 5
    val top5CoursesByCompletionByMdoDF = liveRetiredCourseEnrolmentDF
      .groupBy("courseID", "courseName", "userOrgID", "userOrgName")
      .agg(
        count("userID").alias("enrolledCount"),
        expr("SUM(CASE WHEN dbCompletionStatus=0 THEN 1 ELSE 0 END)").alias("notStartedCount"),
        expr("SUM(CASE WHEN dbCompletionStatus=1 THEN 1 ELSE 0 END)").alias("inProgressCount"),
        expr("SUM(CASE WHEN dbCompletionStatus=2 THEN 1 ELSE 0 END)").alias("completedCount")
      )
      .groupByLimit(Seq("userOrgID"), "completedCount", 5, desc = true)
      .withColumn("jsonData", struct("rowNum", "courseID", "courseName", "userOrgID", "userOrgName", "enrolledCount", "notStartedCount", "inProgressCount", "completedCount"))
      .orderBy(col("completedCount").desc)
      .groupBy("userOrgID")
      .agg(to_json(collect_list("jsonData")).alias("jsonData"))
    Redis.dispatchDataFrame[String]("dashboard_top_5_courses_by_completion_by_org", top5CoursesByCompletionByMdoDF, "userOrgID", "jsonData")

    // Top 5 Content - By completion
    // SELECT courseID, courseName, category, courseOrgName, COUNT(userID) AS enrolled_count,
    // SUM(CASE WHEN dbCompletionStatus=2 THEN 1 ELSE 0 END) AS \"Course Completions\"
    // FROM \"dashboards-user-course-program-progress\"
    // WHERE __time = (SELECT MAX(__time) FROM \"dashboards-user-course-program-progress\")
    // AND userStatus=1 AND courseStatus IN ('Live', 'Retired') $mdo$
    // GROUP BY 1, 2, 3, 4 ORDER BY \"Course Completions\" DESC LIMIT 5
    val top5ContentByCompletionByCbpDF = liveRetiredContentEnrolmentDF
      .groupBy("courseID", "courseName", "courseOrgID")
      .agg(
        count("userID").alias("enrolledCount"),
        expr("SUM(CASE WHEN dbCompletionStatus=2 THEN 1 ELSE 0 END)").alias("completedCount")
      )
      .groupByLimit(Seq("courseOrgID"), "completedCount", 5, desc = true)
      .withColumn("jsonData", struct("rowNum", "courseID", "courseName", "courseOrgID", "completedCount"))
      .orderBy(col("completedCount").desc)
      .groupBy("courseOrgID")
      .agg(to_json(collect_list("jsonData")).alias("jsonData"))
    Redis.dispatchDataFrame[String]("dashboard_top_5_content_by_completion_by_course_org", top5ContentByCompletionByCbpDF, "courseOrgID", "jsonData")

    // Top 5 Content - By enrolments
    // SELECT courseID, courseName, category, courseOrgName, COUNT(userID) AS enrolled_count,
    // FROM \"dashboards-user-course-program-progress\"
    // WHERE __time = (SELECT MAX(__time) FROM \"dashboards-user-course-program-progress\")
    // AND userStatus=1 AND courseStatus IN ('Live', 'Retired') $mdo$
    // GROUP BY 1, 2, 3, 4 ORDER BY \"enrolled_count\" DESC LIMIT 5
    val top5ContentByEnrolmentsByCbpDF = liveRetiredContentEnrolmentDF
      .groupBy("courseID", "courseName", "courseOrgID")
      .agg(
        count("userID").alias("enrolledCount"))
      .groupByLimit(Seq("courseOrgID"), "enrolledCount", 5, desc = true)
      .withColumn("jsonData", struct("rowNum", "courseID", "courseName", "courseOrgID", "enrolledCount"))
      .orderBy(col("enrolledCount").desc)
      .groupBy("courseOrgID")
      .agg(to_json(collect_list("jsonData")).alias("jsonData"))
    Redis.dispatchDataFrame[String]("dashboard_top_5_content_by_enrolments_by_course_org", top5ContentByEnrolmentsByCbpDF, "courseOrgID", "jsonData")


    // Top 5 Courses - By user ratings
    // SELECT courseID, courseName, category, courseOrgName, ROUND(AVG(ratingAverage), 1) AS rating_avg, SUM(ratingCount) AS rating_count
    // FROM \"dashboards-course\" WHERE __time = (SELECT MAX(__time) FROM \"dashboards-course\")
    // AND ratingCount>0 AND ratingAverage<=5.0 AND category='Course' AND courseStatus='Live'
    // GROUP BY 1, 2, 3, 4 ORDER BY rating_count * rating_avg DESC LIMIT 5
    val top5CoursesByRatingDF = ratedLiveCourseDF
      .where(expr("ratingCount>0 AND ratingAverage<=5.0"))
      .withColumn("ratingMetric", expr("ratingCount * ratingAverage"))
      .orderBy(col("ratingMetric").desc)
      .limit(5)
      .select(
        col("courseID"),
        col("courseName"),
        col("courseOrgName"),
        round(col("ratingAverage"), 1).alias("ratingAverage"),
        col("ratingCount")
      )
    val top5CoursesByRatingJson = top5CoursesByRatingDF.toJSON.collectAsList().toString
    Redis.update("dashboard_top_5_courses_by_rating", top5CoursesByRatingJson)

    // Top 5 contents - By user ratings
    // SELECT courseID, courseName, category, courseOrgName, ROUND(AVG(ratingAverage), 1) AS rating_avg, SUM(ratingCount) AS rating_count
    // FROM \"dashboards-course\" WHERE __time = (SELECT MAX(__time) FROM \"dashboards-course\")
    // AND ratingCount>0 AND ratingAverage<=5.0 AND category='Course' AND courseStatus='Live'
    // GROUP BY 1, 2, 3, 4 ORDER BY rating_count * rating_avg DESC LIMIT 5
    val averageRatingsDF = ratedLiveContentDF
      .where(expr("ratingCount > 0 AND ratingAverage <= 5.0"))
      .groupBy("courseOrgID", "courseID", "courseName")
      .agg(round(avg("ratingAverage"), 1).alias("averageRating"), sum("ratingCount").alias("totalRatings"))
    val top5ContentByCourseOrgDF = averageRatingsDF
      .withColumn("rowNum", row_number().over(Window.partitionBy("courseOrgID").orderBy(col("averageRating").desc)))
      .where(col("rowNum") <= 5).select("courseOrgID", "courseID", "courseName", "averageRating", "totalRatings")
    val top5ContentByRatingByOrgDF = top5ContentByCourseOrgDF
      .withColumn("jsonData", struct("courseID", "courseName", "averageRating", "totalRatings"))
      .groupBy("courseOrgID").agg(to_json(collect_list("jsonData")).alias("jsonData"))
    Redis.dispatchDataFrame[String]("dashboard_top_5_content_by_rating_by_course_org", top5ContentByRatingByOrgDF, "courseOrgID", "jsonData")

    //Total ratings by org
    val totalRatingsByCbp = ratedLiveContentDF.groupBy("courseOrgID").agg(count("ratingCount").alias("totalRatings"))
    Redis.dispatchDataFrame[String]("dashboard_content_total_ratings_by_course_org", totalRatingsByCbp, "courseOrgID", "totalRatings")

    //Ratings spread by org
    val ratingsSpreadDF = ratedLiveContentDF.groupBy("courseOrgID").agg(
        sum("count5Star").alias("count5"),
        sum("count4Star").alias("count4"),
        sum("count3Star").alias("count3"),
        sum("count2Star").alias("count2"),
        sum("count1Star").alias("count1"))
      .select(col("courseOrgID"),
        struct(col("count5"), col("count4"), col("count3"), col("count2"), col("count1")).alias("ratingsCount"))
    val ratingsSpreadJsonByCbpDF = ratingsSpreadDF.select(col("courseOrgID"), to_json(col("ratingsCount")).alias("jsonData"))
    Redis.dispatchDataFrame[String]("dashboard_content_ratings_spread_by_course_org", ratingsSpreadJsonByCbpDF, "courseOrgID", "jsonData")

    // Top 5 MDOs - By course completion
    // SELECT userOrgID, userOrgName, COUNT(userID) AS completed_count FROM \"dashboards-user-course-program-progress\"
    // WHERE __time = (SELECT MAX(__time) FROM \"dashboards-user-course-program-progress\")
    // AND userStatus=1 AND category='Course' AND courseStatus IN ('Live', 'Retired') AND dbCompletionStatus=2
    // GROUP BY 1, 2 ORDER BY completed_count DESC LIMIT 5
    val top5MdoByCompletionDF = liveRetiredCourseCompletedDF
      .groupBy("userOrgID", "userOrgName")
      .agg(count("courseID").alias("completedCount"))
      .orderBy(col("completedCount").desc)
      .limit(5)
      .select(
        col("userOrgID"),
        col("userOrgName"),
        col("completedCount")
      )
    val top5MdoByCompletionJson = top5MdoByCompletionDF.toJSON.collectAsList().toString
    Redis.update("dashboard_top_5_mdo_by_completion", top5MdoByCompletionJson)

    // Top 5 MDOs - By courses published
    // SELECT courseOrgID, courseOrgName, COUNT(courseID) AS published_count FROM \"dashboards-course\"
    // WHERE __time = (SELECT MAX(__time) FROM \"dashboards-course\")
    // AND category='Course' AND courseStatus='Live'
    // GROUP BY 1, 2 ORDER BY published_count DESC LIMIT 5
    val top5MdoByLiveCoursesDF = liveCourseDF
      .groupBy("courseOrgID", "courseOrgName")
      .agg(count("courseID").alias("publishedCount"))
      .orderBy(col("publishedCount").desc)
      .limit(5)
      .select(
        col("courseOrgID"),
        col("courseOrgName"),
        col("publishedCount")
      )
    val top5MdoByLiveCoursesJson = top5MdoByLiveCoursesDF.toJSON.collectAsList().toString
    Redis.update("dashboard_top_5_mdo_by_live_courses", top5MdoByLiveCoursesJson)

    // new redis updates - end
  }

  def averageMonthlyActiveUsersDataFrame()(implicit spark: SparkSession, conf: DashboardConfig) : Long = {
    val query = """SELECT ROUND(AVG(daily_count * 1.0), 0) as DAUOutput FROM (SELECT COUNT(DISTINCT(actor_id)) AS daily_count, TIME_FLOOR(__time + INTERVAL '05:30' HOUR TO MINUTE, 'P1D') AS day_start FROM \"telemetry-events-syncts\" WHERE eid='IMPRESSION' AND actor_type='User' AND __time > CURRENT_TIMESTAMP - INTERVAL '30' DAY GROUP BY 2)"""
    var df = druidDFOption(query, conf.sparkDruidRouterHost).orNull
    var averageMonthlyActiveUserCount = 0L
    if (df == null || df.isEmpty) return averageMonthlyActiveUserCount
    else
    {
      df = df.withColumn("DAUOutput", expr("CAST(DAUOutput as LONG)"))
      df = df.withColumn("DAUOutput", col("DAUOutput").cast("long"))
      averageMonthlyActiveUserCount  = df.select("DAUOutput").first().getLong(0)
    }
    averageMonthlyActiveUserCount
  }

  def updateLearnerHomePageData(orgDF: DataFrame, userOrgDF: DataFrame, userCourseProgramCompletionDF: DataFrame, cbpCompletionWithDetailsDF: DataFrame, cbpDetailsWithRatingDF: DataFrame)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {

    //We only want the date here as the intent is to run this part of the script only once a day. The competency metrics
    // script may run a second time if we run into issues and this function should be skipped in that case.
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val currentDateString = dateFormat.format(System.currentTimeMillis())

    val lastRunDate: String = Redis.get("lhp_lastRunDate")
    print("The last run date is " + lastRunDate + "\n")
    print("current Date is" + currentDateString + "\n")

    if(!lastRunDate.equals(currentDateString)) {
      learnerHPRedisCalculations(cbpCompletionWithDetailsDF, cbpDetailsWithRatingDF)
    } else {
      print("This is a second run today and the computation and redis key updates are not required")
    }

    Redis.update("lhp_lastRunDate", currentDateString)

  }

  def learningHoursDiff(learningHoursTillDay0: DataFrame, learningHoursTillDay1: DataFrame, defaultLearningHours: DataFrame, prefix: String): DataFrame = {
    if (learningHoursTillDay0.isEmpty) {
      return defaultLearningHours
    }

    learningHoursTillDay1
      .withColumnRenamed("totalLearningHours", "learningHoursTillDay1")
      .join(learningHoursTillDay0.withColumnRenamed("totalLearningHours", "learningHoursTillDay0"), Seq("userOrgID"), "left")
      .na.fill(0.0, Seq("learningHoursTillDay0", "learningHoursTillDay1"))
      .withColumn("totalLearningHours", expr("learningHoursTillDay1 - learningHoursTillDay0"))
      .withColumn(s"userOrgID", concat(col("userOrgID"), lit(s":${prefix}")))
      .select(s"userOrgID", "totalLearningHours")
  }

  def processLearningHours(courseProgramCompletionWithDetailsDF: DataFrame)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {

    // The courseDuration is coming in seconds from ES, so converting it to hours. Also courseProgress is number of leaf nodes
    // consumed and we should look at completion percentage as the % of learning hours
    val totalLearningHoursTillTodayByOrg = courseProgramCompletionWithDetailsDF
      .filter("userOrgID IS NOT NULL AND TRIM(userOrgID) != ''")
      .groupBy("userOrgID")
      .agg(sum(expr("(completionPercentage / 100) * courseDuration")).alias("totalLearningSeconds"))
      .withColumn("totalLearningHours", col("totalLearningSeconds") / 3600)
      .drop("totalLearningSeconds")

    val totalLearningHoursTillYesterdayByOrg = Redis.getMapAsDataFrame("lhp_learningHoursTillToday", Schema.totalLearningHoursSchema)
      .withColumn("totalLearningHours", col("totalLearningHours").cast(DoubleType))

    val totalLearningHoursTillDayBeforeYesterdayByOrg = Redis.getMapAsDataFrame("lhp_learningHoursTillYesterday", Schema.totalLearningHoursSchema)
      .withColumn("totalLearningHours", col("totalLearningHours").cast(DoubleType))

    //one issue with the learningHoursDiff returning the totalLearningHoursTillTodayByOrg as default if the 1st input DF is
    // empty implies that there would be entries in lhp_learningHours for "orgid":"hours", but I dont see an issue with this
    // I have unit tested the learning hours computation and it looks fine as long as it is run only once a day which we achieving
    // through the lhp_lastRunDate redis key
    val totalLearningHoursTodayByOrg = learningHoursDiff(totalLearningHoursTillYesterdayByOrg, totalLearningHoursTillTodayByOrg, totalLearningHoursTillTodayByOrg, "today")

    val totalLearningHoursYesterdayByOrg = learningHoursDiff(totalLearningHoursTillDayBeforeYesterdayByOrg, totalLearningHoursTillYesterdayByOrg, totalLearningHoursTillTodayByOrg, "yesterday")

    Redis.dispatchDataFrame[Double]("lhp_learningHoursTillToday", totalLearningHoursTillTodayByOrg, "userOrgID", "totalLearningHours", replace = false)
    Redis.dispatchDataFrame[Double]("lhp_learningHoursTillYesterday", totalLearningHoursTillYesterdayByOrg, "userOrgID", "totalLearningHours", replace = false)
    Redis.dispatchDataFrame[Double]("lhp_learningHours", totalLearningHoursYesterdayByOrg, "userOrgID", "totalLearningHours", replace = false)
    Redis.dispatchDataFrame[Double]("lhp_learningHours", totalLearningHoursTodayByOrg, "userOrgID", "totalLearningHours", replace = false)

    // over all
    val totalLearningHoursYesterday: String = Redis.getMapField("lhp_learningHours", "across:today")
    val totalLearningHoursToday: String = totalLearningHoursTodayByOrg.agg(sum("totalLearningHours")).first().getDouble(0).toString

    Redis.updateMapField("lhp_learningHours", "across:yesterday", totalLearningHoursYesterday)
    Redis.updateMapField("lhp_learningHours", "across:today", totalLearningHoursToday)
  }

  def processCertifications(courseProgramCompletionWithDetailsDF: DataFrame)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    val totalCertificationsTillToday = courseProgramCompletionWithDetailsDF
      .where(expr("courseStatus IN ('Live') AND userStatus=1 AND dbCompletionStatus = 2 AND issuedCertificateCount > 0")).count()

    val totalCertificationsTillYesterdayStr = Redis.get("lhp_certificationsTillToday")
    val totalCertificationsTillYesterday = if (totalCertificationsTillYesterdayStr == "") { 0L } else { totalCertificationsTillYesterdayStr.toLong }
    val totalCertificationsTillDayBeforeYesterdayStr = Redis.get("lhp_certificationsTillYesterday")
    val totalCertificationsTillDayBeforeYesterday = if (totalCertificationsTillDayBeforeYesterdayStr == "") { 0L } else { totalCertificationsTillDayBeforeYesterdayStr.toLong }

    val totalCertificationsToday = totalCertificationsTillToday - totalCertificationsTillYesterday
    val totalCertificationsYesterday = totalCertificationsTillYesterday - totalCertificationsTillDayBeforeYesterday

    val currentDayStart = DateTime.now().withTimeAtStartOfDay()

    // The courseCompletionTimestamp is a toLong while getting the completedOn timestamp col from cassandra and it has
    // epoch seconds and not milliseconds. So converting the below to seconds
    val endOfCurrentDay = currentDayStart.plusDays(1).getMillis / 1000
    val startOf7thDay = currentDayStart.minusDays(7).getMillis / 1000

    val certificationsOfTheWeek = courseProgramCompletionWithDetailsDF
      .where(expr(s"courseStatus IN ('Live') AND userStatus=1 AND courseCompletedTimestamp > '${startOf7thDay}' AND courseCompletedTimestamp < '${endOfCurrentDay}' AND dbCompletionStatus = 2 AND issuedCertificateCount > 0"))

    val topNCertifications = certificationsOfTheWeek
      .groupBy("courseID")
      .agg(count("*").alias("courseCount"))
      .orderBy(desc("courseCount"))
      .limit(10)

    Redis.update("lhp_certificationsTillToday", totalCertificationsTillToday.toString)
    Redis.update("lhp_certificationsTillYesterday", totalCertificationsTillYesterday.toString)
    Redis.updateMapField("lhp_certifications", "across:yesterday", totalCertificationsYesterday.toString)
    Redis.updateMapField("lhp_certifications", "across:today", totalCertificationsToday.toString)

    val courseIdsString = topNCertifications
      .agg(concat_ws(",", collect_list("courseID"))).first().getString(0)

    Redis.updateMapField("lhp_trending", "across:certifications", courseIdsString)

    val topNCertificationsByMDO = certificationsOfTheWeek
      .groupBy("userOrgID", "courseID")
      .agg(count("*").alias("courseCount"))
      .orderBy(desc("courseCount"))
      .groupBy("userOrgID")
      .agg(concat_ws(",", collect_list("courseID")).alias("certifications"))
      .withColumn("userOrgID:certifications", concat(col("userOrgID"), lit(":certifications")))
      .limit(10)
    Redis.dispatchDataFrame[String]("lhp_trending", topNCertificationsByMDO, "userOrgID:certifications", "certifications", replace = false)
  }

  def processTrending(allCourseProgramCompletionWithDetailsDF: DataFrame)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {

    val trendingCourses = allCourseProgramCompletionWithDetailsDF
      .filter("dbCompletionStatus IN (0, 1, 2) AND courseStatus = 'Live' AND category = 'Course'")
      .groupBy("courseID")
      .agg(count("*").alias("enrollmentCount"))
      .orderBy(desc("enrollmentCount"))
    val totalCourseCount = trendingCourses.count()
    val courseLimitCount = (totalCourseCount * 0.10).toInt

    val trendingCourseIdsString = trendingCourses.limit(courseLimitCount)
      .agg(concat_ws(",", collect_list("courseID"))).first().getString(0)

    val trendingPrograms = allCourseProgramCompletionWithDetailsDF
      .filter("dbCompletionStatus IN (0, 1, 2) AND courseStatus = 'Live' AND category IN ('Blended Program', 'Curated Program')")
      .groupBy("courseID")
      .agg(count("*").alias("enrollmentCount"))
      .orderBy(desc("enrollmentCount"))
    val totalProgramCount = trendingPrograms.count()
    val programLimitCount = (totalProgramCount * 0.10).toInt

    val trendingProgramIdsString = trendingPrograms.limit(programLimitCount)
      .agg(concat_ws(",", collect_list("courseID"))).first().getString(0)

    val trendingCoursesByOrg = allCourseProgramCompletionWithDetailsDF
      .filter("dbCompletionStatus IN (0, 1, 2) AND courseStatus = 'Live' AND category = 'Course'")
      .groupBy("userOrgID", "courseID")
      .agg(count("*").alias("enrollmentCount"))
      .groupByLimit(Seq("userOrgID"), "enrollmentCount", 50, desc = true)
      .drop("enrollmentCount", "rowNum")

    val trendingCoursesListByOrg = trendingCoursesByOrg
      .groupBy("userOrgID")
      .agg(collect_list("courseID").alias("courseIds"))
      .withColumn("userOrgID:courses", expr("userOrgID"))
      .withColumn("trendingCourseList", concat_ws(",", col("courseIds")))
      .withColumn("userOrgID:courses", concat(col("userOrgID:courses"), lit(":courses")))
      .select("userOrgID:courses", "trendingCourseList")
      .filter(col("userOrgID:courses").isNotNull && col("userOrgID:courses") =!= "")

    val trendingProgramsByOrg = allCourseProgramCompletionWithDetailsDF
      .filter("dbCompletionStatus IN (0, 1, 2) AND courseStatus = 'Live' AND category IN ('Blended Program', 'Curated Program')")
      .groupBy("userOrgID", "courseID")
      .agg(count("*").alias("enrollmentCount"))
      .groupByLimit(Seq("userOrgID"), "enrollmentCount", 50, desc = true)
      .drop("enrollmentCount", "rowNum")

    val trendingProgramsListByOrg = trendingProgramsByOrg
      .groupBy("userOrgID")
      .agg(collect_list("courseID").alias("courseIds"))
      .withColumn("userOrgID:programs", expr("userOrgID"))
      .withColumn("trendingProgramList", concat_ws(",", col("courseIds")))
      .withColumn("userOrgID:programs", concat(col("userOrgID:programs"), lit(":programs")))
      .select("userOrgID:programs", "trendingProgramList")
      .filter(col("userOrgID:programs").isNotNull && col("userOrgID:programs") =!= "")

    val mostEnrolledTag = trendingCourses.limit(courseLimitCount)
      .agg(concat_ws(",", collect_list("courseID"))).first().getString(0)

    Redis.updateMapField("lhp_trending", "across:courses", trendingCourseIdsString)
    Redis.updateMapField("lhp_trending", "across:programs", trendingProgramIdsString)
    Redis.dispatchDataFrame[String]("lhp_trending", trendingCoursesListByOrg, "userOrgID:courses", "trendingCourseList", replace = false)
    Redis.dispatchDataFrame[String]("lhp_trending", trendingProgramsListByOrg, "userOrgID:programs", "trendingProgramList", replace = false)
    //
    Redis.update("lhp_mostEnrolledTag", mostEnrolledTag + "\n")
  }

  def learnerHPRedisCalculations(cbpCompletionWithDetailsDF: DataFrame, cbpDetailsWithRatingDF: DataFrame)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    val courseProgramCompletionWithDetailsDF = cbpCompletionWithDetailsDF.where(expr("category IN ('Course', 'Program')"))

    // do home page data update
    val cbpsUnder30minsDf = cbpDetailsWithRatingDF.where(expr("courseStatus='Live' and courseDuration < 1800 AND category IN ('Course', 'Program')") && !col("courseID").endsWith("_rc")).orderBy(desc("ratingAverage"))
    val coursesUnder30mins = cbpsUnder30minsDf
      .agg(concat_ws(",", collect_list("courseID"))).first().getString(0)
    Redis.updateMapField("lhp_trending", "across:under_30_mins", coursesUnder30mins)

    // calculate and save learning hours to redis
    processLearningHours(courseProgramCompletionWithDetailsDF)

    // calculate and save certifications to redis
    processCertifications(courseProgramCompletionWithDetailsDF)

    // calculate and save trending data
    processTrending(cbpCompletionWithDetailsDF.where(expr("category != 'Program'")))
  }

  def cbpTop10Reviews(allCourseProgramDetailsWithRatingDF: DataFrame)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    // get rating table DF
    val ratingDf = getRatings()
      .join(allCourseProgramDetailsWithRatingDF, col("activityid").equalTo(col("courseID")), "inner")
      .filter(col("review").isNotNull && col("rating").>=("4.5"))
      .select(
        col("activityid").alias("courseID"),
        col("courseOrgID"),
        col("activitytype"),
        col("rating"),
        col("userid").alias("userID"),
        col("review")
      ).orderBy(col("courseOrgID"))

    // assign rank
    val windowSpec = Window.partitionBy("courseOrgID").orderBy(col("rating").desc)
    val resultDF = ratingDf.withColumn("rank", row_number().over(windowSpec))
    val top10PerOrg = resultDF.filter(col("rank") <= 10)

    // create JSON data for top 10 reviews by orgID
    val reviewDF = top10PerOrg
      .groupByLimit(Seq("courseOrgID"), "rank", 10, desc = true)
      .withColumn("jsonData", struct("courseID", "userID", "rating", "review"))
      .groupBy("courseOrgID")
      .agg(to_json(collect_list("jsonData")).alias("jsonData"))

    // write to redis
    Redis.dispatchDataFrame[String]("cbp_top_10_users_reviews_by_org", reviewDF, "courseOrgID", "jsonData")

  }
}