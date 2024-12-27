package org.ekstep.analytics.dashboard.helpers.rating

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig}
import org.ekstep.analytics.framework.FrameworkContext


object RatingReconcilerModel extends AbsDashboardModel {
  implicit val className: String = "org.ekstep.analytics.dashboard.report.user.UserReportModel"

  def courseRatingTableDataFrame()(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
    cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraRatingsTable)
  }

  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    // get user roles data
    var ratingDf = courseRatingTableDataFrame()

    ratingDf = ratingDf.select(
      col("activityid"),
      col("activitytype"),
      col("rating"),
      col("userid")
    )

    val aggDF = ratingDf.groupBy("activityid","activitytype")
      .agg(
        sum("rating").alias("sum_of_total_ratings"),
        count("userid").alias("total_number_of_ratings"),
        sum(when(col("rating") === 1, 1).otherwise(0)).alias("totalcount1stars"),
        sum(when(col("rating") === 2, 1).otherwise(0)).alias("totalcount2stars"),
        sum(when(col("rating") === 3, 1).otherwise(0)).alias("totalcount3stars"),
        sum(when(col("rating") === 4, 1).otherwise(0)).alias("totalcount4stars"),
        sum(when(col("rating") === 5, 1).otherwise(0)).alias("totalcount5stars")
      )

    aggDF.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> conf.cassandraRatingSummaryTable, "keyspace" -> conf.cassandraUserKeyspace))
      .mode("append")
      .save()

    //  closeRedisConnect()
  }
}
