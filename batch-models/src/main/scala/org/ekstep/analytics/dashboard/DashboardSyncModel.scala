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

    val processingTime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").format(timestamp)
    Redis.update("dashboard_update_time", processingTime)

    // obtain and save user org data
    val (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()

    val designationsDF = orgDesignationsDF(userOrgDF)
    Redis.dispatchDataFrame[String]("org_designations", designationsDF, "userOrgID", "org_designations", replace = false)

    // kafkaDispatch(withTimestamp(orgDF, timestamp), conf.orgTopic)
    kafkaDispatch(withTimestamp(userOrgDF, timestamp), conf.userOrgTopic)

    // obtain and save role count data
    val roleDF = roleDataFrame()
    val userOrgRoleDF = userOrgRoleDataFrame(userOrgDF, roleDF)
    val roleCountDF = roleCountDataFrame(userOrgRoleDF)
    kafkaDispatch(withTimestamp(roleCountDF, timestamp), conf.roleUserCountTopic)

    // obtain and save org role count data
    val orgRoleCount = orgRoleCountDataFrame(userOrgRoleDF)
    kafkaDispatch(withTimestamp(orgRoleCount, timestamp), conf.orgRoleUserCountTopic)

    // org user count
    val orgUserCountDF = orgUserCountDataFrame(orgDF, userDF)
    // validate activeOrgCount and orgUserCountDF count
    validate({orgUserCountDF.count()},
    {userOrgDF.filter(expr("userStatus=1 AND userOrgID IS NOT NULL AND userOrgStatus=1")).select("userOrgID").distinct().count()},
    "orgUserCountDF.count() should equal distinct active org count in userOrgDF")

    //obtain and save total karma points of each user
    val karmaPointsDataDF = userKarmaPointsDataFrame()
    .groupBy(col("userid").alias("userID")).agg(sum(col("points")).alias("total_points"))

    val kPointsWithUserOrgDF = karmaPointsDataDF.join(userOrgDF, karmaPointsDataDF("userID") === userOrgDF("userID"), "inner")
    .select(karmaPointsDataDF("*"), userOrgDF("fullName"), userOrgDF("userOrgID"), userOrgDF("userOrgName"), userOrgDF("professionalDetails.designation").alias("designation"), userOrgDF("userProfileImgUrl"))

    val (hierarchyDF, allCourseProgramDetailsWithCompDF, allCourseProgramDetailsDF,
    allCourseProgramDetailsWithRatingDF) = contentDataFrames(orgDF)

    kafkaDispatch(withTimestamp(allCourseProgramDetailsWithRatingDF, timestamp), conf.allCourseTopic)

    // get course competency mapping data, dispatch to kafka to be ingested by druid data-source: dashboards-course-competency
    val allCourseProgramCompetencyDF = allCourseProgramCompetencyDataFrame(allCourseProgramDetailsWithCompDF)
    kafkaDispatch(withTimestamp(allCourseProgramCompetencyDF, timestamp), conf.courseCompetencyTopic)

    // get course completion data, dispatch to kafka to be ingested by druid data-source: dashboards-user-course-program-progress

    val userCourseProgramCompletionDF = userCourseProgramCompletionDataFrame(datesAsLong = true)
    val allCourseProgramCompletionWithDetailsDF = allCourseProgramCompletionWithDetailsDataFrame(userCourseProgramCompletionDF, allCourseProgramDetailsDF, userOrgDF)

    validate({userCourseProgramCompletionDF.count()}, {allCourseProgramCompletionWithDetailsDF.count()}, "userCourseProgramCompletionDF.count() should equal final course progress DF count")
    kafkaDispatch(withTimestamp(allCourseProgramCompletionWithDetailsDF, timestamp), conf.userCourseProgramProgressTopic)

    // org user details redis dispatch
    val (orgRegisteredUserCountMap, orgTotalUserCountMap, orgNameMap) = getOrgUserMaps(orgUserCountDF)
    val activeOrgCount = orgDF.where(expr("orgStatus=1")).count()
    val activeUserCount = userDF.where(expr("userStatus=1")).count()
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

    // update redis data for learner home page
    updateLearnerHomePageData(orgDF, userOrgDF, userCourseProgramCompletionDF)

    // update redis data for dashboards
    dashboardRedisUpdates(orgRoleCount, userDF, allCourseProgramDetailsWithRatingDF, allCourseProgramCompletionWithDetailsDF, allCourseProgramCompetencyDF)

    // update cbp top 10 reviews
    cbpTop10Reviews(allCourseProgramDetailsWithRatingDF)

    Redis.closeRedisConnect()
    }

  def dashboardRedisUpdates(orgRoleCount: DataFrame, userDF: DataFrame, allCourseProgramDetailsWithRatingDF: DataFrame,
  allCourseProgramCompletionWithDetailsDF: DataFrame, allCourseProgramCompetencyDF: DataFrame)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    import spark.implicits._
    // new redis updates - start
    // MDO onboarded, with atleast one MDO_ADMIN/MDO_LEADER
    val orgWithMdoAdminLeaderCount = orgRoleCount.where(expr("role IN ('MDO_ADMIN', 'MDO_LEADER') AND count > 0")).select("orgID").distinct().count()
    val orgWithMdoAdminCount = orgRoleCount.where(expr("role IN ('MDO_ADMIN') AND count > 0")).select("orgID").distinct().count()
    Redis.update("dashboard_org_with_mdo_admin_leader_count", orgWithMdoAdminLeaderCount.toString)
    Redis.update("dashboard_org_with_mdo_admin_count", orgWithMdoAdminCount.toString)

    // mdo-wise registered user count
    val activeUsersByMDODF = userDF.where(expr("userStatus=1")).groupBy("userOrgID").agg(count("*").alias("count"))
    Redis.dispatchDataFrame[Long]("dashboard_user_count_by_user_org", activeUsersByMDODF, "userOrgID", "count")

    // new users registered yesterday
    val usersRegisteredYesterdayDF = userDF
    .withColumn("yesterdayStartTimestamp", date_trunc("day", date_sub(current_timestamp(), 1)).cast("long"))
    .withColumn("todayStartTimestamp", date_trunc("day", current_timestamp()).cast("long"))
    show(usersRegisteredYesterdayDF, "usersRegisteredYesterdayDF")
    val usersRegisteredYesterdayCount = usersRegisteredYesterdayDF
    .where(expr("userCreatedTimestamp >= yesterdayStartTimestamp AND userCreatedTimestamp < todayStartTimestamp and userStatus=1"))
    .count()
    Redis.update("dashboard_new_users_registered_yesterday", usersRegisteredYesterdayCount.toString)
    println(s"dashboard_new_users_registered_yesterday = ${usersRegisteredYesterdayCount}")

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
    println(s"dashboard_cbp_with_live_course_count = ${orgWithLiveCourseCount}")

    // Average rating across all live courses with ratings, and by CBP
    val ratedLiveCourseDF = liveCourseDF.where(col("ratingAverage").isNotNull)
    val ratedLiveCourseModeratedCourseDF = liveCourseModeratedCourseDF.where(col("ratingAverage").isNotNull)
    val avgRatingOverall = ratedLiveCourseDF.agg(avg("ratingAverage").alias("ratingAverage")).select("ratingAverage").first().getDouble(0)
    Redis.update("dashboard_course_average_rating_overall", avgRatingOverall.toString)
    println(s"dashboard_course_average_rating_overall = ${avgRatingOverall}")

    val avgRatingByCBPDF = ratedLiveCourseDF.groupBy("courseOrgID").agg(avg("ratingAverage").alias("ratingAverage"))
    show(avgRatingByCBPDF, "avgRatingByCBPDF")
    Redis.dispatchDataFrame[Double]("dashboard_course_average_rating_by_course_org", avgRatingByCBPDF, "courseOrgID", "ratingAverage")

    val courseModeratedCourseAvgRatingByCBPDF = ratedLiveCourseModeratedCourseDF.groupBy("courseOrgID").agg(avg("ratingAverage").alias("ratingAverage"))
    show(courseModeratedCourseAvgRatingByCBPDF, "courseModeratedCourseAvgRatingByCBPDF")
    Redis.dispatchDataFrame[Double]("dashboard_course_moderated_course_average_rating_by_course_org", courseModeratedCourseAvgRatingByCBPDF, "courseOrgID", "ratingAverage")

    // cbpwise content average rating
    val ratedLiveContentDF = liveContentDF.where(col("ratingAverage").isNotNull)
    val avgContentRatingByCBPDF = ratedLiveContentDF.groupBy("courseOrgID").agg(avg("ratingAverage").alias("ratingAverage"))
    show(avgContentRatingByCBPDF, "avgContentRatingByCBPDF")
    Redis.dispatchDataFrame[Double]("dashboard_content_average_rating_by_course_org", avgContentRatingByCBPDF, "courseOrgID", "ratingAverage")

    // enrollment/not-started/started/in-progress/completion count, live and retired courses
    val liveRetiredContentEnrolmentDF = allCourseProgramCompletionWithDetailsDF.where(expr("courseStatus IN ('Live', 'Retired') AND userStatus=1"))
    val liveRetiredCourseEnrolmentDF = allCourseProgramCompletionWithDetailsDF.where(expr("category='Course' AND courseStatus IN ('Live', 'Retired') AND userStatus=1"))
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
    println("yesterday epoch : "+twentyFourHoursAgoEpochMillis)
    val liveRetiredCourseProgramCompletedYesterdayDF = allCourseProgramCompletionWithDetailsDF.where(expr(s"category IN ('Course', 'Program') AND courseStatus IN ('Live', 'Retired') AND userStatus=1 AND dbCompletionStatus=2 AND courseCompletedTimestamp >= ${twentyFourHoursAgoEpochMillis}"))
    // Calculate twelve months ago
    val twelveMonthsAgo = currentDate.minusMonths(12)
    // Convert to LocalDateTime by adding a time component (midnight)
    val twelveMonthsAgoLocalDateTime = twelveMonthsAgo.atStartOfDay()
    // Get the epoch time in milliseconds with IST offset
    val twelveMonthsAgoEpochMillis = twelveMonthsAgoLocalDateTime.toEpochSecond(java.time.ZoneOffset.ofHoursMinutes(5, 30))
    println(twelveMonthsAgoEpochMillis)
    val liveRetiredCourseEnrolmentsInLast12MonthsDF = allCourseProgramCompletionWithDetailsDF.where(expr(s"category='Course' AND courseStatus IN ('Live', 'Retired') AND userStatus=1 AND courseEnrolledTimestamp >= ${twelveMonthsAgoEpochMillis}"))
    // started + not-started = enrolled
    val liveRetiredCourseNotStartedDF = liveRetiredCourseEnrolmentDF.where(expr("dbCompletionStatus=0"))
    val liveRetiredCourseStartedDF = liveRetiredCourseEnrolmentDF.where(expr("dbCompletionStatus IN (1, 2)"))
    // in-progress + completed = started
    val liveRetiredCourseInProgressDF = liveRetiredCourseStartedDF.where(expr("dbCompletionStatus=1"))
    val liveRetiredCourseCompletedDF = liveRetiredCourseStartedDF.where(expr("dbCompletionStatus=2"))
    val liveRetiredCourseEnrolmentsCompletionsDF = liveRetiredCourseStartedDF.where(expr("dbCompletionStatus IN (0, 1, 2)"))
    // course program completed
    val liveRetiredCourseProgramCompletedDF = liveRetiredCourseProgramEnrolmentDF.where(expr("dbCompletionStatus=2"))
    val liveCourseProgramExcludingModeratedCompletedDF= liveCourseProgramExcludingModeratedEnrolmentDF.where(expr("dbCompletionStatus=2"))

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
    println(s"dashboard_unique_users_enrolled_count = ${enrolmentUniqueUserCount}")
    println(s"dashboard_unique_users_not_started_count = ${notStartedUniqueUserCount}")
    println(s"dashboard_unique_users_started_count = ${startedUniqueUserCount}")
    println(s"dashboard_unique_users_in_progress_count = ${inProgressUniqueUserCount}")
    println(s"dashboard_unique_users_completed_count = ${completedUniqueUserCount}")

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
    show(liveCourseProgramEnrolmentCountsDF, "EnrolmentCounts")
    println(s"dashboard_enrolment_count = ${enrolmentCount}")
    println(s"dashboard_not_started_count = ${notStartedCount}")
    println(s"dashboard_started_count = ${startedCount}")
    println(s"dashboard_in_progress_count = ${inProgressCount}")
    println(s"dashboard_completed_count = ${completedCount}")
    println(s"lp_completed_count = ${landingPageCompletedCount}")
    println(s"lp_completed_yesterday_count = ${landingPageCompletedYesterdayCount}")


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
    val liveRetiredCourseModeratedCourseEnrolmentByCBPDF = liveRetiredCourseModeratedCourseEnrolmentDF.groupBy("courseOrgID").agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
    val liveRetiredContentEnrolmentByCBPDF = liveRetiredContentEnrolmentDF.groupBy("courseOrgID").agg(count("*").alias("count"), countDistinct("userID").alias("uniqueUserCount"))
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

    // get the count for each courseID
    val topContentCountDF = liveCourseProgramExcludingModeratedCompletedDF.groupBy("courseID").agg(count("*").alias("count"))

    // Join count back to main DataFrame
    val liveCourseProgramExcludingModeratedCompletedWithCountDF = liveCourseProgramExcludingModeratedCompletedDF.join(topContentCountDF, "courseID")

    // define a windowspec
    val windowSpec = Window.partitionBy("courseOrgID").orderBy(col("count").desc)

    val topCoursesByCBPDF = liveCourseProgramExcludingModeratedCompletedWithCountDF
    .filter($"category" === "Course")
    .withColumn("sorted_courseIDs", concat_ws(",", collect_set($"courseID").over(windowSpec)))
    .groupBy("courseOrgID")
    .agg(
    concat_ws(",", collect_set($"sorted_courseIDs")).alias("sorted_courseIDs"),
    concat(col("courseOrgID"), lit(":courses")).alias("courseOrgID:content"))
    .select("courseOrgID:content","sorted_courseIDs")
    show(topCoursesByCBPDF, "coursesDF")
    val topProgramsByCBPDF = liveCourseProgramExcludingModeratedCompletedWithCountDF
    .filter($"category" === "Program")
    .withColumn("sorted_courseIDs", concat_ws(",", collect_set($"courseID").over(windowSpec)))
    .groupBy("courseOrgID")
    .agg(
    concat_ws(",", collect_set($"sorted_courseIDs")).alias("sorted_courseIDs"),
    concat(col("courseOrgID"), lit(":programs")).alias("courseOrgID:content"))
    .select("courseOrgID:content","sorted_courseIDs")

    val topAssessmentsByCBPDF = liveCourseProgramExcludingModeratedCompletedWithCountDF
    .filter($"category" === "Standalone Assessment")
    .withColumn("sorted_courseIDs", concat_ws(",", collect_set($"courseID").over(windowSpec)))
    .groupBy("courseOrgID")
    .agg(concat_ws(",", collect_set($"sorted_courseIDs")).alias("sorted_courseIDs"),
    concat(col("courseOrgID"), lit(":assessments")).alias("courseOrgID:content")
    )
    .select("courseOrgID:content","sorted_courseIDs")
    val combinedDFByCBP = topCoursesByCBPDF.union(topProgramsByCBPDF).union(topAssessmentsByCBPDF)

    Redis.dispatchDataFrame[String]("dashboard_top_10_courses_by_completion_by_course_org", combinedDFByCBP, "courseOrgID:content", "sorted_courseIDs")

    // DSR new keys monthly active users and certificate issued yesterday
    val averageMonthlyActiveUsersCount = averageMonthlyActiveUsersDataFrame()
    Redis.update("dashboard_average_monthly_active_users_last_30_days", averageMonthlyActiveUsersCount.toString)
    println(s"dashboard_average_monthly_active_users_last_30_days = ${averageMonthlyActiveUsersCount}")


    println("This is the 24 hours ago start of the day =====")
    println(twentyFourHoursAgoLocalDateTime)
    val tmpDF = liveRetiredCourseModeratedCourseEnrolmentDF.filter($"certificateGeneratedOn".isNotNull && $"certificateGeneratedOn" =!= "").select(col("certificateGeneratedOn"))
    show(tmpDF, "tmpDF to see the datetime type")
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
    println(s"dashboard_courses_enrolled_in_at_least_once = ${coursesEnrolledInCount}")
    println(s"dashboard_courses_completed_at_least_once = ${coursesCompletedCount}")

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
    show(top5UsersByCompletionByMdoDF, "top5UsersByCompletionByMdoDF")
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
    show(top5CoursesByCompletionByMdoDF, "top5CoursesByCompletionByMdoDF")
    Redis.dispatchDataFrame[String]("dashboard_top_5_courses_by_completion_by_org", top5CoursesByCompletionByMdoDF, "userOrgID", "jsonData")

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
    show(top5CoursesByRatingDF, "top5CoursesByRatingDF")
    val top5CoursesByRatingJson = top5CoursesByRatingDF.toJSON.collectAsList().toString
    println(top5CoursesByRatingJson)
    Redis.update("dashboard_top_5_courses_by_rating", top5CoursesByRatingJson)

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
    show(top5MdoByCompletionDF, "top5MdoByCompletionDF")
    val top5MdoByCompletionJson = top5MdoByCompletionDF.toJSON.collectAsList().toString
    println(top5MdoByCompletionJson)
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
    show(top5MdoByLiveCoursesDF, "top5MdoByLiveCoursesDF")
    val top5MdoByLiveCoursesJson = top5MdoByLiveCoursesDF.toJSON.collectAsList().toString
    println(top5MdoByLiveCoursesJson)
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
       averageMonthlyActiveUserCount = df.select("DAUOutput").first().getLong(0)
      }
    println("======222222222222=========")
    println(averageMonthlyActiveUserCount)
    averageMonthlyActiveUserCount
    }

  def updateLearnerHomePageData(orgDF: DataFrame, userOrgDF: DataFrame, userCourseProgramCompletionDF: DataFrame)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {

    val (hierarchyDF, cbpDetailsWithCompDF, cbpDetailsDF,
    cbpDetailsWithRatingDF) = contentDataFrames(orgDF, Seq("Course", "Program", "Blended Program", "Curated Program"))
    val cbpCompletionWithDetailsDF = allCourseProgramCompletionWithDetailsDataFrame(userCourseProgramCompletionDF, cbpDetailsDF, userOrgDF)

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

    print("the current date is :" + currentDateString + "\n")
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
    show(totalLearningHoursTillTodayByOrg, "totalLearningHoursTillTodayByOrg")

    val totalLearningHoursTillYesterdayByOrg = Redis.getMapAsDataFrame("lhp_learningHoursTillToday", Schema.totalLearningHoursSchema)
    .withColumn("totalLearningHours", col("totalLearningHours").cast(DoubleType))
    show(totalLearningHoursTillYesterdayByOrg, "totalLearningHoursTillYesterdayByOrg")

    val totalLearningHoursTillDayBeforeYesterdayByOrg = Redis.getMapAsDataFrame("lhp_learningHoursTillYesterday", Schema.totalLearningHoursSchema)
    .withColumn("totalLearningHours", col("totalLearningHours").cast(DoubleType))
    show(totalLearningHoursTillDayBeforeYesterdayByOrg, "totalLearningHoursTillDayBeforeYesterdayByOrg")

    //one issue with the learningHoursDiff returning the totalLearningHoursTillTodayByOrg as default if the 1st input DF is
    // empty implies that there would be entries in lhp_learningHours for "orgid":"hours", but I dont see an issue with this
    // I have unit tested the learning hours computation and it looks fine as long as it is run only once a day which we achieving
    // through the lhp_lastRunDate redis key
    val totalLearningHoursTodayByOrg = learningHoursDiff(totalLearningHoursTillYesterdayByOrg, totalLearningHoursTillTodayByOrg, totalLearningHoursTillTodayByOrg, "today")
    show(totalLearningHoursTodayByOrg, "totalLearningHoursTodayByOrg")

    val totalLearningHoursYesterdayByOrg = learningHoursDiff(totalLearningHoursTillDayBeforeYesterdayByOrg, totalLearningHoursTillYesterdayByOrg, totalLearningHoursTillTodayByOrg, "yesterday")
    show(totalLearningHoursYesterdayByOrg, "totalLearningHoursYesterdayByOrg")

    Redis.dispatchDataFrame[Double]("lhp_learningHoursTillToday", totalLearningHoursTillTodayByOrg, "userOrgID", "totalLearningHours", replace = false)
    Redis.dispatchDataFrame[Double]("lhp_learningHoursTillYesterday", totalLearningHoursTillYesterdayByOrg, "userOrgID", "totalLearningHours", replace = false)
    Redis.dispatchDataFrame[Double]("lhp_learningHours", totalLearningHoursYesterdayByOrg, "userOrgID", "totalLearningHours", replace = false)
    Redis.dispatchDataFrame[Double]("lhp_learningHours", totalLearningHoursTodayByOrg, "userOrgID", "totalLearningHours", replace = false)

    // over all
    val totalLearningHoursYesterday: String = Redis.getMapField("lhp_learningHours", "across:today")
    val totalLearningHoursToday: String = totalLearningHoursTodayByOrg.agg(sum("totalLearningHours")).first().getDouble(0).toString

    println("learning across yesterday :" + totalLearningHoursYesterday)
    Redis.updateMapField("lhp_learningHours", "across:yesterday", totalLearningHoursYesterday)
    println("learning across today :" + totalLearningHoursToday)
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

    println("certifications till today :" + totalCertificationsTillToday)
    Redis.update("lhp_certificationsTillToday", totalCertificationsTillToday.toString)
    println("certifications till yesterday :" + totalCertificationsTillYesterday)
    Redis.update("lhp_certificationsTillYesterday", totalCertificationsTillYesterday.toString)
    println("certifications across yesterday :" + totalCertificationsYesterday)
    Redis.updateMapField("lhp_certifications", "across:yesterday", totalCertificationsYesterday.toString)
    println("certifications across today :" + totalCertificationsToday)
    Redis.updateMapField("lhp_certifications", "across:today", totalCertificationsToday.toString)

    val courseIdsString = topNCertifications
    .agg(concat_ws(",", collect_list("courseID"))).first().getString(0)

    print("trending certifications :" +courseIdsString + "\n")
    Redis.updateMapField("lhp_trending", "across:certifications", courseIdsString)

    val topNCertificationsByMDO = certificationsOfTheWeek
    .groupBy("userOrgID", "courseID")
    .agg(count("*").alias("courseCount"))
    .orderBy(desc("courseCount"))
    .groupBy("userOrgID")
    .agg(concat_ws(",", collect_list("courseID")).alias("certifications"))
    .withColumn("userOrgID:certifications", concat(col("userOrgID"), lit(":certifications")))
    .limit(10)
    show(topNCertificationsByMDO, "topNCertificationsByMDO")
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

    print("trending courses :" +trendingCourseIdsString + "\n")
    Redis.updateMapField("lhp_trending", "across:courses", trendingCourseIdsString)
    print("trending programs :" +trendingProgramIdsString + "\n")
    Redis.updateMapField("lhp_trending", "across:programs", trendingProgramIdsString)
    show(trendingCoursesListByOrg, "trendingCoursesListByOrg")
    Redis.dispatchDataFrame[String]("lhp_trending", trendingCoursesListByOrg, "userOrgID:courses", "trendingCourseList", replace = false)
    show(trendingProgramsListByOrg, "trendingProgramsListByOrg")
    Redis.dispatchDataFrame[String]("lhp_trending", trendingProgramsListByOrg, "userOrgID:programs", "trendingProgramList", replace = false)
    //
    print("most enrolled tag :" + mostEnrolledTag)
    Redis.update("lhp_mostEnrolledTag", mostEnrolledTag + "\n")
    }

  def learnerHPRedisCalculations(cbpCompletionWithDetailsDF: DataFrame, cbpDetailsWithRatingDF: DataFrame)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    val courseProgramCompletionWithDetailsDF = cbpCompletionWithDetailsDF.where(expr("category IN ('Course', 'Program')"))

    // do home page data update
    val cbpsUnder30minsDf = cbpDetailsWithRatingDF.where(expr("courseStatus='Live' and courseDuration < 1800 AND category IN ('Course', 'Program')") && !col("courseID").endsWith("_rc")).orderBy(desc("ratingAverage"))
    show(cbpsUnder30minsDf, "cbpsUnder30minsDf")
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
    var ratingDf = getRatings()
    // filter records by courseID and rating greater than 4.5
    ratingDf = ratingDf
    .join(allCourseProgramDetailsWithRatingDF, col("activityid").equalTo(col("courseID")), "inner")
    .filter(col("review").isNotNull && col("rating").>=("4.5"))

    ratingDf = ratingDf.select(
    col("activityid").alias("courseID"),
    col("courseOrgID"),
    col("activitytype"),
    col("rating"),
    col("userid").alias("userID"),
    col("review")
    ).orderBy(col("courseOrgID"))

    show(ratingDf)

    // assign rank
    val windowSpec = Window.partitionBy("courseOrgID").orderBy(col("rating").desc)
    val resultDF = ratingDf.withColumn("rank", row_number().over(windowSpec))
    val top10PerOrg = resultDF.filter(col("rank") <= 10) // Show the result top10PerOrg.show()

    show(top10PerOrg, "top10PerOrg")

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
