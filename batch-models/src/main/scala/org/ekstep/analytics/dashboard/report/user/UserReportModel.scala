package org.ekstep.analytics.dashboard.report.user

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig, Redis}
import org.ekstep.analytics.framework.FrameworkContext


object UserReportModel extends AbsDashboardModel {
  implicit val className: String = "org.ekstep.analytics.dashboard.report.user.UserReportModel"
  override def name() = "UserReportModel"

  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    val today = getDate()

    // get user roles data
    val userRolesDF = roleDataFrame().groupBy("userID").agg(concat_ws(", ", collect_list("role")).alias("role")) // return - userID, role

    val (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()

    val orgHierarchyData = orgHierarchyDataframe()
    val weeklyClapsDF = learnerStatsDataFrame()
    val karmaPointsDF = cache.load("userKarmaPointsSummary")
      .withColumnRenamed("userid", "userID")
    val userData = userOrgDF
      .join(userRolesDF, Seq("userID"), "left")
      .join(karmaPointsDF.select("userID","total_points"), Seq("userID"), "left")
      .join(broadcast(orgHierarchyData), Seq("userOrgID"), "left")
      .dropDuplicates("userID")
      .withColumn("Tag", concat_ws(", ", col("additionalProperties.tag")))
    val userDataWithKarmaPoints = userData
      .join(weeklyClapsDF, userData("userID") === weeklyClapsDF("userid"), "left")
      .select(userData("*"), weeklyClapsDF("total_claps").alias("weekly_claps_day_before_yesterday"))

    val reportPath = s"${conf.userReportPath}/${today}"
    //generateReport(fullReportDF, s"${reportPath}-full")
    val mdoWiseReportDF = userDataWithKarmaPoints
      .withColumn("Report_Last_Generated_On", currentDateTime)
      .select(
        col("fullName").alias("Full_Name"),
        col("professionalDetails.designation").alias("Designation"),
        col("personalDetails.primaryEmail").alias("Email"),
        col("personalDetails.mobile").alias("Phone_Number"),
        col("professionalDetails.group").alias("Group"),
        col("Tag"),
        col("ministry_name").alias("Ministry"),
        col("dept_name").alias("Department"),
        col("userOrgName").alias("Organization"),
        from_unixtime(col("userCreatedTimestamp"), dateFormat).alias("User_Registration_Date"),
        col("role").alias("Roles"),
        col("personalDetails.gender").alias("Gender"),
        col("personalDetails.category").alias("Category"),
        col("additionalProperties.externalSystem").alias("External_System"),
        col("additionalProperties.externalSystemId").alias("External_System_Id"),
        col("userOrgID").alias("mdoid"),
        col("Report_Last_Generated_On"),
        from_unixtime(col("userOrgCreatedDate"), dateFormat).alias("MDO_Created_On"),
        col("userVerified").alias("Verified Karmayogi"),
        col("weekly_claps_day_before_yesterday")
      ).coalesce(1)
    // Repartition by mdo_id and write to CSV
    generateReport(mdoWiseReportDF, reportPath,"mdoid", "UserReport")
    // to be removed once new security job is created
    if (conf.reportSyncEnable) {
      syncReports(s"${conf.localReportDir}/${reportPath}", reportPath)
    }
    val df_warehouse = userDataWithKarmaPoints
      .withColumn("marked_as_not_my_user", when(col("userProfileStatus") === "NOT-MY-USER", true).otherwise(false))
      .withColumn("data_last_generated_on", currentDateTime)
      .select(
        col("userID").alias("user_id"),
        col("userOrgID").alias("mdo_id"),
        col("userStatus").alias("status"),
        coalesce(col("total_points"), lit(0)).alias("no_of_karma_points"),
        col("fullName").alias("full_name"),
        col("professionalDetails.designation").alias("designation"),
        col("personalDetails.primaryEmail").alias("email"),
        col("personalDetails.mobile").alias("phone_number"),
        col("professionalDetails.group").alias("groups"),
        col("Tag").alias("tag"),
        col("userVerified").alias("is_verified_karmayogi"),
        date_format(from_unixtime(col("userCreatedTimestamp")), dateTimeFormat).alias("user_registration_date"),
        col("role").alias("roles"),
        col("personalDetails.gender").alias("gender"),
        col("personalDetails.category").alias("category"),
        col("userCreatedBy").alias("created_by_id"),
        col("additionalProperties.externalSystem").alias("external_system"),
        col("additionalProperties.externalSystemId").alias("external_system_id"),
        col("weekly_claps_day_before_yesterday"),
        col("marked_as_not_my_user"),
        col("data_last_generated_on")
      )

    generateReport(df_warehouse.coalesce(1), s"${reportPath}-warehouse")

    Redis.closeRedisConnect()

  }
}


