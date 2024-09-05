package org.ekstep.analytics.dashboard

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.ekstep.analytics.framework.FrameworkContext


object TestUtil extends Serializable {

  def main(model: AbsDashboardModel): Unit = {
    val config = testModelConfig()
    implicit val (spark, sc, fc) = DashboardUtil.Test.getSessionAndContext(model.name(), config)
    val res = DashboardUtil.Test.time(test(config, model))
    Console.println("Time taken to execute script", res._1)
    spark.stop()
  }

  def test(config: Map[String, AnyRef], model: AbsDashboardModel)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    model.parseConfigAndProcessData(System.currentTimeMillis(), config)
  }

  def testModelConfig(): Map[String, AnyRef] = {
    val sideOutput = Map(
      "brokerList" -> "192.168.3.249:9092",
      "compression" -> "none",
      "topics" -> Map(
        "roleUserCount" -> "dev.dashboards.role.count",
        "orgRoleUserCount" -> "dev.dashboards.org.role.count",
        "allCourses" -> "dev.dashboards.course",
        "userCourseProgramProgress" -> "dev.dashboards.user.course.program.progress",
        "fracCompetency" -> "dev.dashboards.competency.frac",
        "courseCompetency" -> "dev.dashboards.competency.course",
        "expectedCompetency" -> "dev.dashboards.competency.expected",
        "declaredCompetency" -> "dev.dashboards.competency.declared",
        "competencyGap" -> "dev.dashboards.competency.gap",
        "userOrg" -> "dev.dashboards.user.org",
        "org" -> "dev.dashboards.org",
        "userAssessment" -> "dev.dashboards.user.assessment",
        "assessment" -> "dev.dashboards.assessment"
      )
    )
    val modelParams = Map(
      "debug" -> "true",
      "validation" -> "true",

      "redisHost" -> "192.168.3.249",
      "redisPort" -> "6379",
      "redisDB" -> "12",

      "sparkCassandraConnectionHost" -> "192.168.3.211",
      "sparkDruidRouterHost" -> "192.168.3.91",
      "sparkElasticsearchConnectionHost" -> "192.168.3.211",
      "fracBackendHost" -> "frac-dictionary.karmayogi.nic.in",
      "sparkMongoConnectionHost" -> "192.168.3.178",

      "solutionIDs" -> "",
      "mlMongoDatabase" -> "ml-survey",
      "surveyCollection" -> "solutions",
      "reportConfigCollection" -> "dataProductConfigurations",
      "gracePeriod" -> "2",
      "baseUrlForEvidences" -> "www.https://igotkarmayogi.gov.in/",
      "mlReportPath" -> "standalone-reports/ml-report",
      "includeExpiredSolutionIDs" -> "true",
      "mlSparkDruidRouterHost" -> "192.168.3.91",
      "mlSparkMongoConnectionHost" -> "192.168.3.178",
      "SurveyQuestionReportBatchSize" -> "2000",
      "SurveyStatusReportBatchSize" -> "20000",
      "ObservationQuestionReportBatchSize" -> "2000",
      "ObservationStatusReportBatchSize" -> "15000",

      "appPostgresHost" -> "192.168.3.178:5432",
      "appPostgresUsername" -> "sunbird",
      "appPostgresCredential" -> "sunbird",
      "appPostgresSchema" -> "sunbird",

      "appOrgHierarchyTable" -> "org_hierarchy_v4",
      "postgresCompetencyTable" -> "data_node",
      "postgresCompetencyHierarchyTable" -> "node_mapping",

      "dwPostgresHost" -> "192.168.3.211:5432",
      "dwPostgresUsername" -> "postgres",
      "dwPostgresCredential" -> "Password@12345678",
      "dwPostgresSchema" -> "warehouse",

      "dwUserTable" -> "user_detail",
      "dwCourseTable" -> "content",
      "dwEnrollmentsTable" -> "user_enrolments",
      "dwOrgTable" -> "org_hierarchy",
      "dwAssessmentTable" -> "assessment_detail",
      "dwBPEnrollmentsTable" -> "bp_enrolments",

      "dwKcmDictionaryTable" -> "kcm_dictionary",
      "dwKcmContentTable" -> "kcm_content_mapping",
      "dwCBPlanTable" -> "cb_plan",
      "dwContentResourceTable" -> "content_resource",

      "cassandraUserKeyspace" -> "sunbird",
      "cassandraCourseKeyspace" -> "sunbird_courses",
      "cassandraHierarchyStoreKeyspace" -> "dev_hierarchy_store",
      "cassandraUserFeedKeyspace" -> "sunbird_notifications",

      "cassandraUserTable" -> "user",
      "cassandraUserRolesTable" -> "user_roles",
      "cassandraOrgTable" -> "organisation",
      "cassandraUserEnrolmentsTable" -> "user_enrolments",
      "cassandraContentHierarchyTable" -> "content_hierarchy",
      "cassandraRatingSummaryTable" -> "ratings_summary",
      "cassandraRatingsTable" -> "ratings",
      "cassandraOrgHierarchyTable" -> "org_hierarchy",
      "cassandraCourseBatchTable" -> "course_batch",
      "cassandraLearnerStatsTable" -> "learner_stats",
      "cassandraKarmaPointsTable" -> "user_karma_points",
      "cassandraHallOfFameTable" -> "mdo_karma_points",
      "cassandraUserAssessmentTable" -> "user_assessment_data",
      "cassandraKarmaPointsLookupTable" -> "user_karma_points_credit_lookup",
      "cassandraKarmaPointsTable" -> "user_karma_points",
      "cassandraKarmaPointsSummaryTable" -> "user_karma_points_summary",
      "cassandraUserFeedTable" -> "notification_feed",
      "cassandraLearnerStatsTable" -> "learner_stats",
      "cassandraAcbpTable" -> "cb_plan",
      "cassandraLearnerLeaderBoardLookupTable" -> "learner_leaderboard_lookup",
      "cassandraLearnerLeaderBoardTable" -> "learner_leaderboard",

      "mongoDatabase" -> "nodebb",
      "mongoDBCollection" -> "objects",

      "key" -> "aws_storage_key",
      "secret" -> "aws_storage_secret",
      "store" -> "gs",
      "container" -> "igot",

      "platformRatingSurveyId" -> "1696404440829",
      "cutoffTime" -> "60.0",
      "reportSyncEnable" -> "true",
      "mdoIDs" -> "",

      "userReportPath" -> "standalone-reports/user-report",
      "userEnrolmentReportPath" -> "standalone-reports/user-enrollment-report",
      "courseReportPath" -> "standalone-reports/course-report",
      "cbaReportPath" -> "standalone-reports/cba-report",
      "taggedUsersPath" -> "tagged-users/",
      "standaloneAssessmentReportPath" -> "standalone-reports/user-assessment-report-cbp",
      "blendedReportPath" -> "standalone-reports/blended-program-report",
      "orgHierarchyReportPath" -> "standalone-reports/org-hierarchy-report",
      "acbpReportPath" -> "standalone-reports/cbp-report",
      "acbpMdoEnrolmentReportPath" -> "standalone-reports/cbp-report-mdo-enrolment",
      "acbpMdoSummaryReportPath" -> "standalone-reports/cbp-report-mdo-summary",
      "kcmReportPath" -> "standalone-reports/kcm-report",
      "commsConsoleReportPath" -> "standalone-reports/comms-console",
      "validationReportPath"->"standalone-reports/validation-report",
      "commsConsolePrarambhEmailSuffix" -> ".kb@karmayogi.in",
      "commsConsoleNumDaysToConsider" -> "15",
      "commsConsoleNumTopLearnersToConsider" -> "60",
      "commsConsolePrarambhTags" -> "rojgaar,rozgaar,rozgar",
      "commsConsolePrarambhCbpIds" -> "do_113882965067743232154,do_1137468666262241281756,do_1139032976499261441156",
      "commsConsolePrarambhNCount" -> "2",
      "nationalLearningWeekStart" -> "",
      "nationalLearningWeekEnd" -> "",
      "cassandraNLWUserLeaderboardTable" -> "nlw_user_leaderboard",
      "cassandraNLWMdoLeaderboardTable" -> "nlw_mdo_leaderboard",

      "prefixDirectoryPath" -> "standalone-reports",
      "destinationDirectoryPath" -> "standalone-reports/merged",
      "localReportDir" -> "/mount/data/analytics/reports",
      "directoriesToSelect" -> "blended-program-report-mdo,cbp-report-mdo-summary,course-report,cba-report,cbp-report-mdo-enrolment,user-report,user-enrollment-report",
      "password" -> "123456",

      "sideOutput" -> sideOutput
    )
    modelParams
  }

}
