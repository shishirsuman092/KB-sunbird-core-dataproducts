package org.ekstep.analytics.dashboard.odcs

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time.{Instant, LocalDate, ZoneOffset, ZonedDateTime, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.util.UUID
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig, Redis}
import org.ekstep.analytics.framework.FrameworkContext

  object OdcsRecomendationModel extends AbsDashboardModel {

    implicit val className: String = "org.ekstep.analytics.dashboard.odcs.OdcsRecomendationModel"

    override def name() = "OdcsRecomendationModel"
    def processData(timestamp: Long) (implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
      try{
        val enrolmentsDF = warehouseCache.load(conf.dwEnrollmentsTable)
        val userDF = warehouseCache.load(conf.dwUserTable)
        val ratingDraftDF = cache.load("rating")  // Contains content_id, rating

        val completionDF = enrolmentsDF
          .groupBy("content_id")
          .agg(count("user_id").alias("total_enrolments"),
            sum(when(col("user_consumption_status") === "completed", 1).otherwise(0)).alias("completed_enrolments"))
          .withColumn("completion_percentage", (col("completed_enrolments") / col("total_enrolments")) * 100)

        val ratingDF = ratingDraftDF
          .filter(col("activitytype") === "Course")
          .groupBy("activityid")
          .agg(
            count("*").alias("rating_count"),
            sum("rating").alias("total_rating"))
          .withColumn("avg_rating", col("total_rating") / col("rating_count"))
          .select(
            col("activityid").alias("content_id"),
            col("rating_count"),
            col("avg_rating"))

        val enrolmentsWithMDO = enrolmentsDF.join(userDF, Seq("user_id"), "inner").select("mdo_id", "content_id")

        val contentStatsDF = enrolmentsWithMDO
          .join(completionDF, Seq("content_id"), "left")
          .join(ratingDF, Seq("content_id"), "left")
          .groupBy("mdo_id", "content_id")
          .agg(first("completion_percentage").alias("completion_percentage"), first("avg_rating").alias("avg_rating"),
            count("content_id").alias("total_enrolments"))
          .na.fill(0, Seq("completion_percentage", "avg_rating","total_enrolments"))

        val windowSpec = Window.partitionBy("mdo_id")
          .orderBy(
            col("completion_percentage").desc,
            col("avg_rating").desc,
            col("total_enrolments").desc)

        val rankedContentDF = contentStatsDF
          .withColumn("rank", row_number().over(windowSpec))
          .filter(col("rank") <= 15)

        val finalDF = rankedContentDF
          .groupBy("mdo_id")
          .agg(collect_list("content_id").alias("top_content_ids"))
          .withColumn("top_15_content_ids", concat_ws(",", col("top_content_ids")))
          .select("mdo_id", "top_15_content_ids")

        Redis.dispatchDataFrame[Long]("odcs_course_recomendation", finalDF, "mdo_id", "top_15_content_ids")
      }catch {
      case e: Exception =>
        println(s"Error occurred during OdcsRecomendationModel processing: ${e.getMessage}", e)
        System.exit(1)
    }
    }
  }
