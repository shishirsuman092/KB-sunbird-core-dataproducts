package org.ekstep.analytics.dashboard.helpers

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.ekstep.analytics.dashboard.{DashboardConfig, DashboardConfigParser, DashboardUtil}
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.framework.FrameworkContext


object ValidateDerivedTest extends Serializable {

  def main(args: Array[String]): Unit = {
    val config = testModelConfig()
    implicit val (spark, sc, fc) = DashboardUtil.Test.getSessionAndContext("ValidateDerivedTest", config)
    val res = DashboardUtil.Test.time(test(config));
    Console.println("Time taken to execute script", res._1);
    spark.stop();
  }

  def test(config: Map[String, AnyRef])(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {

    implicit val conf: DashboardConfig = DashboardConfigParser.parseConfig(config)
    if (conf.debug == "true") debug = true // set debug to true if explicitly specified in the config
    if (conf.validation == "true") validation = true // set validation to true if explicitly specified in the config

    val rootPath = "/home/analytics/wfs-analysis"

    val date = "2023-12-04"

    val jsonFileWFS1 = s"${rootPath}/derived-${date}-wfs1.json.gz"
    val jsonFileWFS2 = s"${rootPath}/derived-${date}-wfs2.json.gz"

    val userDayRootStatsDF1 = process(jsonFileWFS1, s"${rootPath}/out-${date}/summaryDF1")
    val userDayRootStatsDF2 = process(jsonFileWFS2, s"${rootPath}/out-${date}/summaryDF2")

    //var userDF = userDataFrame()
    //userDF = userDF.drop("userProfileDetails")

    val userID1 = userDayRootStatsDF1.select("uid").distinct()
    val userID2 = userDayRootStatsDF2.select("uid").distinct()

    var usersIn1NotIn2 = userID1.join(userID2, Seq("uid"), "leftanti")
    var usersIn2NotIn1 = userID2.join(userID1, Seq("uid"), "leftanti")

    //usersIn1NotIn2 = usersIn1NotIn2.join(userDF, userDF.col("userID") === usersIn1NotIn2.col("uid"), "left")
    //usersIn2NotIn1 = usersIn2NotIn1.join(userDF, userDF.col("userID") === usersIn2NotIn1.col("uid"), "left")

    csvWrite(usersIn1NotIn2.coalesce(1), s"${rootPath}/out-${date}/diff1minus2")
    csvWrite(usersIn2NotIn1.coalesce(1), s"${rootPath}/out-${date}/diff2minus1")

  }

  def process(jsonFile: String, outPath: String)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): DataFrame = {
    val summaryDF = getSummaryEventDF(jsonFile)
    val userDayRootStatsDF = analyse(summaryDF, outPath)
    userDayRootStatsDF
  }

  def analyse(summaryDF: DataFrame, path: String): DataFrame = {
    val rootSummaryDF = summaryDF.where(expr("dimensions.type='app'"))

    // mid debug
    val midCountDF = summaryDF.groupBy("mid").agg(count("*").alias("count")).where(expr("count > 1")).orderBy(col("count").desc)
    csvWrite(midCountDF.coalesce(1), s"${path}/midCountDF")

    val midRootCountDF = rootSummaryDF.groupBy("mid").agg(count("*").alias("count")).where(expr("count > 1")).orderBy(col("count").desc)
    csvWrite(midRootCountDF.coalesce(1), s"${path}/midRootCountDF")

    // group by day stats
    val dayStatsDF = getStats(summaryDF.groupBy("dateIST")).orderBy("dateIST")
    csvWrite(dayStatsDF.coalesce(1), s"${path}/dayStatsDF")

    val dayRootStatsDF = getStats(rootSummaryDF.groupBy("dateIST")).orderBy("dateIST")
    csvWrite(dayRootStatsDF.coalesce(1), s"${path}/dayRootStatsDF")

    // group by user stats
    val userDayStatsDF = getStats(summaryDF.groupBy("dateIST", "uid")).orderBy("dateIST", "uid")
    csvWrite(userDayStatsDF.coalesce(1), s"${path}/userDayStatsDF")

    val userDayRootStatsDF = getStats(rootSummaryDF.groupBy("dateIST", "uid")).orderBy("dateIST", "uid")
    csvWrite(userDayRootStatsDF.coalesce(1), s"${path}/userDayRootStatsDF")

    userDayRootStatsDF
  }

  def getStats(groupedDF: RelationalGroupedDataset): DataFrame = {
    val df = groupedDF.agg(
      count("*").alias("count"),

      // from_utc_timestamp(min("ets"), "Asia/Kolkata").alias("etsMin"),
      // from_utc_timestamp(max("ets"), "Asia/Kolkata").alias("etsMax"),

      // from_utc_timestamp(min("syncts"), "Asia/Kolkata").alias("synctsMin"),
      // from_utc_timestamp(max("syncts"), "Asia/Kolkata").alias("synctsMax"),

      count("mid").alias("mid #"),
      countDistinct("mid").alias("mid # uniq"),

      count("uid").alias("uid #"),
      countDistinct("uid").alias("uid # uniq"),

      count("dimensions.did").alias("did #"),
      countDistinct("dimensions.did").alias("did # uniq"),

      count("dimensions.channel").alias("channel #"),
      countDistinct("dimensions.channel").alias("channel # uniq"),

      count("object.id").alias("object #"),
      countDistinct("object.id").alias("object # uniq"),

      sum("edata.eks.interact_events_count").alias("interaction #"),
      sum("edata.eks.time_spent").alias("timeSpent")
    )

    df
  }

  def getSummaryEventDF(jsonFile: String)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): DataFrame = {
    var df = spark.read.json(jsonFile)

    df = df
      .withColumn("ets", to_timestamp(expr("ROUND(ets / 1000, 0)").cast(LongType)))
      .withColumn("syncts", to_timestamp(expr("ROUND(syncts / 1000, 0)").cast(LongType)))
      .withColumn("dateTimeIST", from_utc_timestamp(col("ets"), "Asia/Kolkata"))
      .withColumn("dateIST", to_date(col("dateTimeIST"),"yyyy-MM-dd"))

    df
  }

  def testModelConfig(): Map[String, AnyRef] = {
    val sideOutput = Map(
      "brokerList" -> "",
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
        "userOrg" -> "dev.dashboards.user.org"
      )
    )
    val modelParams = Map(
      "debug" -> "true",
      "validation" -> "true",

      "redisHost" -> "",
      "redisPort" -> "6379",
      "redisDB" -> "12",

      "sparkCassandraConnectionHost" -> "192.168.3.200",
      "sparkDruidRouterHost" -> "192.168.3.21",
      "sparkElasticsearchConnectionHost" -> "192.168.3.90",
      "fracBackendHost" -> "frac-dictionary.igotkarmayogi.gov.in",

      "cassandraUserKeyspace" -> "sunbird",
      "cassandraCourseKeyspace" -> "sunbird_courses",
      "cassandraHierarchyStoreKeyspace" -> "prod_hierarchy_store",

      "cassandraUserTable" -> "user",
      "cassandraUserRolesTable" -> "user_roles",
      "cassandraOrgTable" -> "organisation",
      "cassandraUserEnrolmentsTable" -> "user_enrolments",
      "cassandraContentHierarchyTable" -> "content_hierarchy",
      "cassandraRatingSummaryTable" -> "ratings_summary",

      "sideOutput" -> sideOutput
    )
    modelParams
  }

}
