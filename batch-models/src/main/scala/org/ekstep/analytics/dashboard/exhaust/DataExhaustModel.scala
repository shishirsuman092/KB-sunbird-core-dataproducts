package org.ekstep.analytics.dashboard.exhaust

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.ekstep.analytics.dashboard.DashboardUtil._
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
    show(enrolmentDF, "enrolmentDF")
    cache.write(enrolmentDF, "enrolment")

    val batchDF = cassandraTableAsDataFrame(conf.cassandraCourseKeyspace, conf.cassandraCourseBatchTable)
    show(batchDF, "batchDF")
    cache.write(batchDF, "batch")

    val userAssessmentDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraUserAssessmentTable)
    show(userAssessmentDF, "userAssessmentDF")
    cache.write(userAssessmentDF, "userAssessment")

    val hierarchyDF = cassandraTableAsDataFrame(conf.cassandraHierarchyStoreKeyspace, conf.cassandraContentHierarchyTable)
    show(hierarchyDF, "hierarchyDF")
    cache.write(hierarchyDF, "hierarchy")

    val ratingSummaryDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraRatingSummaryTable)
    show(ratingSummaryDF, "ratingSummaryDF")
    cache.write(ratingSummaryDF, "ratingSummary")

    val acbpDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraAcbpTable)
    show(acbpDF, "acbpDF")
    cache.write(acbpDF, "acbp")

    val ratingDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraRatingsTable)
    show(ratingDF, "ratingDF")
    cache.write(ratingDF, "rating")

    val roleDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraUserRolesTable)
    show(roleDF, "roleDF")
    cache.write(roleDF, "role")

    // ES content
    val primaryCategories = Seq("Course","Program","Blended Program","Curated Program","Standalone Assessment","CuratedCollections","Moderated Course")
    val shouldClause = primaryCategories.map(pc => s"""{"match":{"primaryCategory.raw":"${pc}"}}""").mkString(",")
    val fields = Seq("identifier", "name", "primaryCategory", "status", "reviewStatus", "channel", "duration", "leafNodesCount", "lastPublishedOn", "lastStatusChangedOn", "createdFor", "competencies_v5", "programDirectorName","language")
    val arrayFields = Seq("createdFor","language")
    val fieldsClause = fields.map(f => s""""${f}"""").mkString(",")
    val query = s"""{"_source":[${fieldsClause}],"query":{"bool":{"should":[${shouldClause}]}}}"""
    val esContentDF = elasticSearchDataFrame(conf.sparkElasticsearchConnectionHost, "compositesearch", query, fields, arrayFields)
    show(esContentDF, "esContentDF")
    cache.write(esContentDF, "esContent")

    val orgDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraOrgTable)
    show(orgDF, "orgDF")
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
    show(orgHierarchyDF, "orgHierarchyDF")
    cache.write(orgHierarchyDF, "orgHierarchy")
    show(orgPostgresDF, "orgCompleteHierarchyDF")
    cache.write(orgPostgresDF, "orgCompleteHierarchy")

    val userDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraUserTable)
    show(userDF, "userDF")
    cache.write(userDF, "user")

    val learnerLeaderboardDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraLearnerLeaderBoardTable)
    show(learnerLeaderboardDF, "learnerLeaderboardDF")
    cache.write(learnerLeaderboardDF, "learnerLeaderBoard")

    val userKarmaPointsDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraKarmaPointsTable)
    show(userKarmaPointsDF, "Karma Points data")
    cache.write(userKarmaPointsDF, "userKarmaPoints")

    val userKarmaPointsSummaryDF = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraKarmaPointsSummaryTable)
    show(userKarmaPointsSummaryDF, "userKarmaPointsSummaryDF")
    cache.write(userKarmaPointsSummaryDF, "userKarmaPointsSummary")
  }
}

