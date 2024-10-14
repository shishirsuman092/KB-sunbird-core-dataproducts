package org.ekstep.analytics.dashboard.report.monthly.requests

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig}
import org.ekstep.analytics.framework.FrameworkContext

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter


object MonthlyRequestModel extends AbsDashboardModel {
  implicit val className: String = "org.ekstep.analytics.dashboard.report.monthly.requests.MonthlyRequestModel"

  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {

    val dateTimeFormatString = DateTimeFormatter.ofPattern(dateTimeFormat)
    val now = LocalDateTime.now()
    val fromDate = now.minusMonths(1).withDayOfMonth(1).withHour(0).withMinute(0).withSecond(0).format(dateTimeFormatString)
    val toDate = now.withDayOfMonth(1).withHour(23).withMinute(59).withSecond(59).format(dateTimeFormatString)

    val monthStart = date_format(date_trunc("MONTH", add_months(current_date(), -1)), s"${dateFormat} 00:00:00")
    val monthEnd = date_format(last_day(add_months(current_date(), -1)), s"${dateFormat} 23:59:59")

    val userDayCountWallOfFameData = userDayCountWallOfFameDataFrame(fromDate, toDate)
    val karmaPointsWallOfFameData = cassandraTableAsDataFrame("sunbird", "user_karma_points")
      .filter(col("credit_date") >= monthStart && col("credit_date") <= monthEnd)
      .groupBy(col("userid")).agg(sum(col("points")).alias("total_points"), max(col("credit_date")).alias("last_credit_date"))

    val mobileVersionsData = mobileVersionsDataFrame(fromDate)

    userDayCountWallOfFameData.repartition(1).write.mode(SaveMode.Overwrite).format("csv").option("header", true).save(s"${conf.localReportDir}/monthly-requests/wallOfFame-user-day-count")
    karmaPointsWallOfFameData.repartition(1).write.mode(SaveMode.Overwrite).format("csv").option("header", true).save(s"${conf.localReportDir}/monthly-requests/wallOfFame-karma-points-data")
    mobileVersionsData.repartition(1).write.mode(SaveMode.Overwrite).format("csv").option("header", true).save(s"${conf.localReportDir}/monthly-requests/mobile-versions-data")


  }
}



