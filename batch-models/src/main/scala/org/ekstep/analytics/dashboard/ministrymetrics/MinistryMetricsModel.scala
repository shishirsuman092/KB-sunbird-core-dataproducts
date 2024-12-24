package org.ekstep.analytics.dashboard.ministrymetrics

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.DashboardUtil._
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

    // Join the user and enrolment data
    val joinUserDF = enrolmentDF.join(userDF, enrolmentDF("user_id") === userDF("user_ID"), "inner") // Inner join on user_id

    // Join with the org_hierarchy data to get ministryID for all DF operations

    val joinedWithMinistryIDDF = joinUserDF.join(org_hierarchyDF, userDF("user_org_id") === org_hierarchyDF("mdo_id"), "left_outer")

    val certificateResultDF = joinedWithMinistryIDDF
      .groupBy("ministry")
      .agg(countDistinct("certificate_id").alias("certificateCount"))


    // Aggregate and create enrolmentResultDF
    val enrolmentResultDF = joinedWithMinistryIDDF
      .groupBy("ministry")
      .agg(count("user_ID").alias("enrolmentCount"))


    // Create userCountDF
    val userCountDF = userDF.join(org_hierarchyDF, userDF("user_org_id") === org_hierarchyDF("mdo_id"), "left_outer")
      .groupBy("ministry")
      .agg(count("user_ID").alias("userCount"))


    // certificateResultDF.join(ministryNamesDF, Seq("ministry"), "inner").select(col("ministryID"),col("certificateCount"))
    //  .repartition(1).write.mode(SaveMode.Overwrite).format("csv").option("header", true).save("/tmp/certificateCountData")
    //userCountDF.join(ministryNamesDF, Seq("ministry"), "inner").select(col("ministryID"),col("userCount"))
    //    .repartition(1).write.mode(SaveMode.Overwrite).format("csv").option("header", true).save("/tmp/userCountData")
    //  enrolmentResultDF.join(ministryNamesDF, Seq("ministry"), "inner").select(col("ministryID"),col("enrolmentCount"))
    //    .repartition(1).write.mode(SaveMode.Overwrite).format("csv").option("header", true).save("/tmp/enrolmentCountData")

    val finalCertificateCountDF = certificateResultDF.join(ministryNamesDF, Seq("ministry"), "inner").select(col("ministryID"), coalesce(col("certificateCount"), lit(0)).alias("certificateCount"))
    val finalUserCountDF = userCountDF.join(ministryNamesDF, Seq("ministry"), "inner").select(col("ministryID"), coalesce(col("userCount"), lit(0)).alias("userCount"))
    val finalEnrolmentCountDF = enrolmentResultDF.join(ministryNamesDF, Seq("ministry"), "inner").select(col("ministryID"), coalesce(col("enrolmentCount"), lit(0)).alias("enrolmentCount"))

    Redis.dispatchDataFrame[Int]("dashboard_rolled_up_user_count", finalUserCountDF, "ministryID", "userCount")
    // Redis.dispatchDataFrame[Int]("dashboard_rolled_up_login_percent_last_24_hrs", combinedMinistryMetricsDF, "ministryID", "loginSumValue")
    Redis.dispatchDataFrame[Double]("dashboard_rolled_up_certificates_generated_count", finalCertificateCountDF, "ministryID", "certificateCount")
    Redis.dispatchDataFrame[Double]("dashboard_rolled_up_enrolment_content_count",finalEnrolmentCountDF, "ministryID", "enrolmentCount")
  }
}