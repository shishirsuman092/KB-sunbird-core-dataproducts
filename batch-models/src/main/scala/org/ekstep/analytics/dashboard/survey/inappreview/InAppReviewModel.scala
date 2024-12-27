package org.ekstep.analytics.dashboard.survey.inappreview

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, current_date, expr, lit}
import java.time.{LocalDate, LocalDateTime, ZoneOffset}
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig}
import org.ekstep.analytics.framework.FrameworkContext


object InAppReviewModel extends AbsDashboardModel {
  implicit val className: String = "org.ekstep.analytics.dashboard.survey.inappreview.InAppReviewModel"

  override def name() = "InAppReviewModel"
  def processData(timestamp: Long) (implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {


    //gives data from cache for weekly claps
    val weeklyClapsDF = cache.load("weeklyClaps")

    // calculate end of the week to set an expiry date for the feeds
    def endOfWeek(today: LocalDate): LocalDate = {
      // Calculate the end of the week (Sunday)
      today.plusDays(7 - today.getDayOfWeek().getValue())
    }
    def endOfDay(date: LocalDate): LocalDateTime = {
      // Get the start of the next day
      val startOfNextDay = date.plusDays(1).atStartOfDay()
      // Subtract one nanosecond to get the end of the current day
      startOfNextDay.minusNanos(1)
    }
    // Today's date
    val todayDate = LocalDate.now()

    // Calculate expireOn date
    val expireOnDate = endOfWeek(todayDate)
    print("Expire on date " + expireOnDate)

    //convert expireOn date to epoch seconds as expected by feed table
    val expireOnDateTime = endOfDay(expireOnDate)
    val expireOnEpochms = (expireOnDateTime.atOffset(ZoneOffset.UTC).toEpochSecond())*1000
    print("Expire on epochms "+expireOnEpochms)

    //fetch the userids based on the condition
    val filteredDF = weeklyClapsDF.filter(col("claps_updated_this_week") && expr("date_format(last_claps_updated_on, 'yyyy-MM-dd')") === current_date())
      .select("userid")

    //add required columns for feed data
    val resultDF = filteredDF.withColumn("expireon", lit(expireOnEpochms))
      .withColumn("category", lit("InAppReview"))
      .withColumn("id", expr("uuid()").cast("string"))
      .withColumn("createdby", lit("weekly_claps"))
      .withColumn("createdon", current_date())
      .withColumn("action", lit("{}"))
      .withColumn("priority", lit(1))
      .withColumn("status", lit("unread"))
      .withColumn("updatedby", lit(null).cast("string"))
      .withColumn("updatedon", lit(null).cast("date"))
      .withColumn("version", lit("v1"))

    // write the dataframe to cassandra user_feed table
    resultDF.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> conf.cassandraUserFeedKeyspace , "table" -> conf.cassandraUserFeedTable))
      .mode("append")
      .save()
  }

}

