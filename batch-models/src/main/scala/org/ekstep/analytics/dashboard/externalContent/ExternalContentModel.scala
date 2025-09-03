package org.ekstep.analytics.dashboard.externalContent

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard._
import org.ekstep.analytics.framework.FrameworkContext

import java.io.Serializable

object ExternalContentModel extends AbsDashboardModel{

  implicit val className: String = "org.ekstep.analytics.dashboard.externalContent.ExternalContentModel"

  override def name() = "ExternalContentModel"

  object Schema extends Serializable {
    val illumineExternalPropertiesSchema: StructType = StructType(Seq(
      StructField("pedgogUserId", StringType, true),
      StructField("assessment_score", StringType, true),
      StructField("assessment_date", StringType, true),
      StructField("batch_name", StringType, true),
      StructField("batchId", StringType, true),
      StructField("batch_location", StringType, true),
      StructField("facilitator_MDO", StringType, true),
      StructField("facilitators", ArrayType(StringType, true))
    ))
  }
  /**
   * Master method, does all the work, fetching, processing and dispatching
   *
   * @param timestamp unique timestamp from the start of the processing
   */

  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    try {

      // get user org mapping
      val (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()

      // get external content
      val externalCourseEnrolmentsDF = marketPlaceEnrolments()

      val externalCourseEnrolmentsDetailsDF = externalCourseEnrolmentsDF
        .filter(col("courseid").isin("ext_11431676755830374411"))
        .withColumn("enrolment_details", from_json(col("additional_properties"), Schema.illumineExternalPropertiesSchema))

      externalCourseEnrolmentsDetailsDF.show(truncate = false)




    } catch {
      case e: Exception =>
        println(s"Error occurred during DataExhaustModel processing: ${e.getMessage}", e)
        System.exit(1)
    }
  }
}
