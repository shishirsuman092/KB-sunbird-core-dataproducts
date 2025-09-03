package org.ekstep.analytics.adhoc

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig}
import org.ekstep.analytics.framework._

import scala.util.{Failure, Success, Try}

/**
 * Model for processing dashboard data
 */
object ContentFirstPublishedDate extends AbsDashboardModel {

  implicit val className: String = "org.ekstep.analytics.adhoc.ContentFirstPublishedDate"

  override def name() = "ContentFirstPublishedDate"

  /**
   * Master method, does all the work, fetching, processing and dispatching
   *
   * @param timestamp unique timestamp from the start of the processing
   */
  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    /**
     * Content ID
     * Content Type
     * First Published On
     * Last Published On
     * Status
     */

    // Configuration
    val ES_HOST = "10.175.4.10"
    val ES_INDEX = "kp_audit*"
    val BATCH_SIZE = 10
    val RETRY_ATTEMPTS = 3
    val BACKOFF_TIME = 200 // milliseconds#Define schema
    //for the nested logRecord
    val log_record_schema = StructType(Seq(
      StructField("properties", StructType(Seq(
        StructField("lastPublishedOn", StructType(Seq(
          StructField("ov", StringType),
          StructField("nv", StringType)
        ))),
        StructField("status", StructType(Seq(
          StructField("ov", StringType),
          StructField("nv", StringType))))
      )))
    ))

    //Define main schema
    val main_schema = StructType(Seq(
      StructField("objectId", StringType),
      StructField("objectType", StringType),
      StructField("label", StringType),
      StructField("graphId", StringType),
      StructField("userId", StringType),
      StructField("requestId", StringType),
      StructField("logRecord", StringType),
      StructField("operation", StringType),
      StructField("createdOn", LongType)
    ))

    val log_schema = StructType(Seq(
      StructField("objectId", StringType),
      StructField("createdOn", LongType),
      StructField("lastPublishedOn_old", StringType),
      StructField("lastPublishedOn_new", StringType),
      StructField("status_old", StringType),
      StructField("status_old", StringType)
    ))


    // read course data
    //val contentDF = cache.load("esContent")
    val (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()

    val (hierarchyDF, allCourseProgramDetailsWithCompDF, allCourseProgramDetailsDF,
    allCourseProgramDetailsWithRatingDF) = contentDataFrames(orgDF, Seq("Course", "Program", "Blended Program", "Curated Program", "Standalone Assessment", "CuratedCollections", "Moderated Course"))


    // Main execution
    val liveCourseIds = allCourseProgramDetailsDF
      .filter(col("courseStatus") === "Live")
      .select("courseID")
      .distinct()
      .limit(BATCH_SIZE)
      .collect()
      .map(_.getAs[String]("courseID"))

    // Process all courseIds in a single batch
    liveCourseIds.foreach(x => processAuditLogs(Seq(x)))

    def processAuditLogs(courseIds: Seq[String]): Unit = {
      // Fields to fetch from ES
      val fields = Seq("objectId", "objectType", "label", "graphId", "userId",
        "requestId", "logRecord", "operation", "createdOn")

      // Create batch query for multiple courseIds
      val batchQuery =
        s"""
        {
          "query": {
            "bool": {
              "should": [
                ${courseIds.map(id => s"""{"term": {"objectId": "$id"}}""").mkString(",")}
              ]
            }
          }
        }"""

      println("-----------")
      println(batchQuery)
      println("-----------")
      def queryWithRetry(attempt: Int = 1): Unit = {
        Try {
          val esContentAuditDF = elasticSearchDataFrame(ES_HOST, ES_INDEX, batchQuery, fields)

          esContentAuditDF.show()

          val logs = esContentAuditDF.withColumn("parsed_log", from_json(col("logRecord"), log_record_schema))
            .filter(col("parsed_log.properties.status.nv") === "Live")
            .select(
            col("objectId"),
            col("createdOn"),
            col("parsed_log.properties.lastPublishedOn.ov").alias("lastPublishedOn_old"),
            col("parsed_log.properties.lastPublishedOn.nv").alias("lastPublishedOn_new"),
            col("parsed_log.properties.status.ov").alias("status_old"),
            col("parsed_log.properties.status.nv").alias("status_new")
          )

          logs.show()

          if(!logs.isEmpty) {
            //warehouseCache.write(logs.coalesce(1), "contentLog")
            logs.coalesce(1).write.mode(SaveMode.Append).option("compression", "uncompressed").format("avro").save("/mount/data/analytics/warehouse/contentLog")
          }
        } match {
          case Success(_) =>
            println(s"Successfully processed batch of ${courseIds.size} courses")

          case Failure(e) if attempt < RETRY_ATTEMPTS =>
            println(s"Attempt $attempt failed: ${e.getMessage}")
            Thread.sleep(BACKOFF_TIME * attempt)
            queryWithRetry(attempt + 1)

          case Failure(e) =>
            println(s"Failed to process batch after $RETRY_ATTEMPTS attempts: ${e.getMessage}")
        }
      }

      // Execute the query with retry logic
      queryWithRetry()
    }


  }

}
