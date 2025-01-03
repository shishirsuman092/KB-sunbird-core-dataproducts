package org.ekstep.analytics.dashboard.ministrymetrics

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig, Redis}
import org.ekstep.analytics.framework.FrameworkContext

object MinistryMetricsModel extends AbsDashboardModel {

  implicit val className: String = "org.ekstep.analytics.dashboard.MinistryMetricsModel"

  override def name() = "MinistryMetricsModel"

  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {

    import spark.implicits._

    val org_hierarchyDF = cache.load("orgHierarchy")
    val ministryNamesDF = org_hierarchyDF.select(col("mdo_name").alias("ministry"), col("mdo_id").alias("ministryID"))
    val enrolmentDF = warehouseCache.load("user_enrolments")
    val userDF = warehouseCache.load("user_detail").withColumnRenamed("mdo_id", "user_org_id").withColumnRenamed("user_id", "user_ID").filter(col("status") === 1)
    val query =  raw"""SELECT DISTINCT(uid) as user_ID FROM \"summary-events\" WHERE dimensions_type='app' AND __time > CURRENT_TIMESTAMP - INTERVAL '24' HOUR"""
    val userLogin24HrDF = druidDFOption(query, conf.sparkDruidRouterHost).orNull
    if (userLogin24HrDF == null) {
      print("Empty dataframe: userLogin24HrDF")
    }
    val userLogin24HrWithDetailsDF = userLogin24HrDF.join(userDF, Seq("user_ID"), "inner").select(col("user_ID"), col("user_org_id"))

    // Join the user and enrolment data
    val joinUserDF = enrolmentDF.join(userDF, enrolmentDF("user_id") === userDF("user_ID"), "inner") // Inner join on user_id

    // Join with the org_hierarchy data to get ministryID for all DF operations

    val joinedWithMinistryIDDF = joinUserDF.join(org_hierarchyDF, userDF("user_org_id") === org_hierarchyDF("mdo_id"), "left_outer")

    val certificateMinistryDF = joinedWithMinistryIDDF
      .groupBy("ministry")
      .agg(countDistinct("certificate_id").alias("certificateCount"))

    val certificateDeptDF = joinedWithMinistryIDDF
      .groupBy("department")
      .agg(countDistinct("certificate_id").alias("certificateCount"))
      .select(col("department").alias("ministry"), col("certificateCount"))

    val certificateOrgDF = joinedWithMinistryIDDF
      .groupBy("organization")
      .agg(countDistinct("certificate_id").alias("certificateCount"))
      .select(col("organization").alias("ministry"), col("certificateCount"))


    val certificateResultDF = certificateMinistryDF.union(certificateDeptDF).union(certificateOrgDF)

    // Aggregate and create enrolmentResultDF

    val enrolmentMinistrytDF = joinedWithMinistryIDDF
      .groupBy("ministry")
      .agg(count("user_ID").alias("enrolmentCount"))

    val enrolmentDeptDF = joinedWithMinistryIDDF
      .groupBy("department")
      .agg(count("user_ID").alias("enrolmentCount"))
      .select(col("department").alias("ministry"), col("enrolmentCount"))

    val enrolmentOrgDF = joinedWithMinistryIDDF
      .groupBy("organization")
      .agg(count("user_ID").alias("enrolmentCount"))
      .select(col("organization").alias("ministry"), col("enrolmentCount"))

    val enrolmentResultDF = enrolmentMinistrytDF.union(enrolmentDeptDF).union(enrolmentOrgDF)

    // Create userCountDF
    val userCountMinistryDF = userDF.join(org_hierarchyDF, userDF("user_org_id") === org_hierarchyDF("mdo_id"), "left_outer")
      .groupBy("ministry")
      .agg(count("user_ID").alias("userCount"))

    val userCountDeptDF = userDF.join(org_hierarchyDF, userDF("user_org_id") === org_hierarchyDF("mdo_id"), "left_outer")
      .groupBy("department")
      .agg(count("user_ID").alias("userCount"))
      .select(col("department").alias("ministry"), col("userCount"))

    val userCountOrgDF = userDF.join(org_hierarchyDF, userDF("user_org_id") === org_hierarchyDF("mdo_id"), "left_outer")
      .groupBy("organization")
      .agg(count("user_ID").alias("userCount"))
      .select(col("organization").alias("ministry"), col("userCount"))

    val userCountDF = userCountMinistryDF.union(userCountDeptDF).union(userCountOrgDF)

    val userLoggedInLast24HrCountMinistryDF = userLogin24HrWithDetailsDF.join(org_hierarchyDF, userLogin24HrWithDetailsDF("user_org_id") === org_hierarchyDF("mdo_id"), "left_outer")
      .groupBy("ministry")
      .agg(count("user_ID").alias("userLogin24HrCount"))

    val userLoggedInLast24HrCountDeptDF = userLogin24HrWithDetailsDF.join(org_hierarchyDF, userLogin24HrWithDetailsDF("user_org_id") === org_hierarchyDF("mdo_id"), "left_outer")
      .groupBy("department")
      .agg(count("user_ID").alias("userLogin24HrCount"))
      .select(col("department").alias("ministry"), col("userLogin24HrCount"))
    val userLoggedInLast24HrCountOrgDF = userLogin24HrWithDetailsDF.join(org_hierarchyDF, userLogin24HrWithDetailsDF("user_org_id") === org_hierarchyDF("mdo_id"), "left_outer")
      .groupBy("organization")
      .agg(count("user_ID").alias("userLogin24HrCount"))
      .select(col("organization").alias("ministry"), col("userLogin24HrCount"))

    val userLoggedInLast24HrCount = userLoggedInLast24HrCountMinistryDF.union(userLoggedInLast24HrCountDeptDF).union(userLoggedInLast24HrCountOrgDF)


    val finalCertificateCountDF = certificateResultDF.join(ministryNamesDF, Seq("ministry"), "inner").select(col("ministryID"), coalesce(col("certificateCount"), lit(0)).alias("certificateCount"))
    val finalEnrolmentCountDF = enrolmentResultDF.join(ministryNamesDF, Seq("ministry"), "inner").select(col("ministryID"), coalesce(col("enrolmentCount"), lit(0)).alias("enrolmentCount"))
    val finalUserCountDF = userCountDF.join(ministryNamesDF, Seq("ministry"), "inner").select(col("ministryID"), coalesce(col("userCount"), lit(0)).alias("userCount"))
    val finalUserLoggedInLast24HrCountDF = userLoggedInLast24HrCount.join(ministryNamesDF, Seq("ministry"), "inner").select(col("ministryID"), coalesce(col("userLogin24HrCount"), lit(0)).alias("userLogin24HrCount"))

    Redis.dispatchDataFrame[Double]("dashboard_rolled_up_certificates_generated_count", finalCertificateCountDF, "ministryID", "certificateCount")
    Redis.dispatchDataFrame[Double]("dashboard_rolled_up_enrolment_content_count",finalEnrolmentCountDF, "ministryID", "enrolmentCount")
    Redis.dispatchDataFrame[Int]("dashboard_rolled_up_user_count", finalUserCountDF, "ministryID", "userCount")
    Redis.dispatchDataFrame[Int]("dashboard_rolled_up_login_percent_last_24_hrs", finalUserLoggedInLast24HrCountDF, "ministryID", "userLogin24HrCount")
  }
}