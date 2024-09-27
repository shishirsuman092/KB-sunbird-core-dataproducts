package org.ekstep.analytics.dashboard.weekly.claps

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig}
import org.ekstep.analytics.framework.FrameworkContext
import org.ekstep.analytics.framework.util.JobLogger


object WeeklyClapsModel extends AbsDashboardModel {

  implicit val className: String = "org.ekstep.analytics.dashboard.weekly.claps.WeeklyClapsModel"

  override def name() = "WeeklyClapsModel"

  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {

    // get weekStart, weekEnd and dataTillDate(previous day) from today's date
    val (weekStart, weekEnd, weekEndTime, dataTillDate) = getThisWeekDates()
//    val weekStart = ""     //for manual testing
//    val weekEndTime = ""

    //get existing weekly-claps data
    val existingWeeklyClapsDF = learnerStatsDataFrame()
    // get platform engagement data from summary-events druid datasource
    val platformEngagementDF = usersPlatformEngagementDataframe(weekStart, weekEndTime)

    val joinedWithExistingDF = existingWeeklyClapsDF.join(platformEngagementDF, Seq("userid"), "full")
      .withColumn("w4", map(
        lit("timespent"), when(col("platformEngagementTime").isNull, 0).otherwise(col("platformEngagementTime")),
        lit("numberOfSessions"), when(col("sessionCount").isNull, 0).otherwise(col("sessionCount"))
      ))

    var df = joinedWithExistingDF

    val condition = col("w4")("timespent") >= conf.cutoffTime && !col("claps_updated_this_week")

    if(dataTillDate.equals(weekEnd) && !dataTillDate.equals(df.select(col("last_updated_on")))) {
      JobLogger.log("Started weekend updates")
      df = df.select(
        col("w2").alias("w1"),
        col("w3").alias("w2"),
        col("w4").alias("w3"),
        col("w4"),
        col("total_claps"),
        col("userid"),
        col("platformEngagementTime"),
        col("sessionCount"),
        col("claps_updated_this_week"),
        col("last_claps_updated_on")
      )
        .withColumn("total_claps", when(col("w4")("timespent") < conf.cutoffTime, 0).otherwise(col("total_claps")))
        .withColumn("total_claps", when(condition, col("total_claps") + 1).otherwise(col("total_claps")))
        .withColumn("last_updated_on", lit(dataTillDate))
        .withColumn("claps_updated_this_week", lit(false))
        .withColumn("w4", map(lit("timespent"), lit(0.0), lit("numberOfSessions"), lit(0)))

      JobLogger.log("Completed weekend updates")

    } else {
      df = df.withColumn("total_claps", when(condition, col("total_claps") + 1).otherwise(col("total_claps")))
        .withColumn("claps_updated_this_week", when(condition, lit(true)).otherwise(col("claps_updated_this_week")))
    }

    df = df.withColumn("total_claps", when(col("total_claps").isNull, 0).otherwise(col("total_claps")))
      .withColumn("claps_updated_this_week", when(col("claps_updated_this_week").isNull, false).otherwise(col("claps_updated_this_week")))
      .withColumn("last_claps_updated_on", when(condition, currentDateTime).otherwise(col("last_claps_updated_on")))

    df = df.drop("platformEngagementTime","sessionCount")

    writeToCassandra(df, conf.cassandraUserKeyspace, conf.cassandraLearnerStatsTable)
  }
}