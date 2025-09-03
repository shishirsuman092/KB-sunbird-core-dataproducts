package org.ekstep.analytics.adhoc

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig, Redis}
import org.ekstep.analytics.framework._

import java.text.SimpleDateFormat

/**
 * Model for processing dashboard data
 */
object ContentHierarchy extends AbsDashboardModel {

  implicit val className: String = "org.ekstep.analytics.adhoc.ContentHierarchy"

  override def name() = "ContentHierarchy"

  /**
   * Master method, does all the work, fetching, processing and dispatching
   *
   * @param timestamp unique timestamp from the start of the processing
   */
  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {

    val processingTime = new SimpleDateFormat(s"${dateFormat}'T'${timeFormat}'Z'").format(timestamp)
    Redis.update("dashboard_update_time", processingTime)

    // obtain and save user org data
    val (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()

    val (hierarchyDF, allCourseProgramDetailsWithCompDF, allCourseProgramDetailsDF,
    allCourseProgramDetailsWithRatingDF) = contentDataFrames(orgDF,Seq("Course","Moderated Course"))


    show(allCourseProgramDetailsWithCompDF,"allCourseProgramDetailsWithCompDF")

    val hierarchySchema = Schema.makeHierarchySchema(false, true, false)
    val hierarchy = hierarchyDF.withColumn("data", from_json(col("hierarchy"), hierarchySchema))
      .withColumn("name", col("data.name"))
      .withColumn("status", col("data.status"))
      .withColumn("primaryCategory", col("data.primaryCategory"))
      .withColumn("contentType", col("data.contentType"))
      .withColumn("objectType", col("data.objectType"))
      .withColumn("createdOn", col("data.createdOn"))
      .withColumn("mimeTypesCount", col("data.mimeTypesCount"))
      .withColumn("competencies_v6", col("data.competencies_v6"))
      .select(
        col("identifier").alias("hierarchyID"),
        col("name"),
        col("status"),
        col("primaryCategory"),
        col("contentType"),
        col("objectType"),
        col("createdOn"),
        col("competencies_v6")
      )
      .drop("hierarchy")
      .drop("data")

    show(hierarchy.select(col("primaryCategory")).distinct())
    println(hierarchy.filter(col("competencies_v6").isNull).count())
    val hierarchyDFWithMissingCompetencyV6 = hierarchy.filter(col("competencies_v6").isNull)

    val courseHierarchyDFWithMissingCompetencyV6DF = hierarchyDFWithMissingCompetencyV6.join(allCourseProgramDetailsWithCompDF, hierarchy("hierarchyID") === allCourseProgramDetailsWithCompDF("courseID"), "left")
      .filter(col("courseStatus").isin("Live"))
      .select(
        col("hierarchyID"),
        col("courseName"),
        col("courseOrgID"),
        col("courseStatus"),
        col("courseLastPublishedOn"),
        col("name"),
        col("status"),
        col("primaryCategory"),
        col("contentType"),
        col("objectType"),
        col("createdOn")
      )

    show(courseHierarchyDFWithMissingCompetencyV6DF, "hierarchyDFWithMissingCompetencyV6")

    courseHierarchyDFWithMissingCompetencyV6DF.coalesce(1).write.mode(SaveMode.Overwrite).format("csv").option("header", true).save(s"/tmp/courseHierarchyDFWithMissingCompetencyV6")

    /*val hierarchyDFWithSCROM = hierarchy.filter(col("mimeTypesCount").contains("application/vnd.ekstep.html-archive") and col("primaryCategory") === "Course" and col("status").isin("Live","Retired"))
    val hierarchyDFWithoutSCROM = hierarchy.filter(not(col("mimeTypesCount").contains("application/vnd.ekstep.html-archive")) and col("primaryCategory") === "Course" and col("status").isin("Live","Retired"))



    val courseHierarchyDFWithSCROM = hierarchyDFWithSCROM.join(allCourseProgramDetailsWithCompDF, hierarchy("hierarchyID") === allCourseProgramDetailsWithCompDF("courseID"), "left")
      .filter(col("courseStatus").isin("Live","Retired"))
      .select(
        col("hierarchyID"),
        col("courseName"),
        col("courseOrgID"),
        col("courseStatus"),
        col("courseLastPublishedOn"),
        col("name"),
        col("status"),
        col("primaryCategory"),
        col("contentType"),
        col("objectType"),
        col("createdOn"),
        col("mimeTypesCount")
      )

    show(courseHierarchyDFWithSCROM,"courseHierarchyDFWithSCROM")

    val courseHierarchyDFWithoutSCROM = hierarchyDFWithoutSCROM.join(allCourseProgramDetailsWithCompDF, hierarchy("hierarchyID") === allCourseProgramDetailsWithCompDF("courseID"), "left")
      .filter(col("courseStatus").isin("Live","Retired"))
      .select(
        col("hierarchyID"),
        col("courseName"),
        col("courseOrgID"),
        col("courseStatus"),
        col("courseLastPublishedOn"),
        col("name"),
        col("status"),
        col("primaryCategory"),
        col("contentType"),
        col("objectType"),
        col("createdOn"),
        col("mimeTypesCount")
      )

    show(courseHierarchyDFWithoutSCROM,"courseHierarchyDFWithoutSCROM")

    courseHierarchyDFWithSCROM.coalesce(1).write.mode(SaveMode.Overwrite).format("csv").option("header", true).save(s"/tmp/courseHierarchyDFWithSCROM")
    courseHierarchyDFWithoutSCROM.coalesce(1).write.mode(SaveMode.Overwrite).format("csv").option("header", true).save(s"/tmp/courseHierarchyDFWithoutSCROM")
*/
    show(hierarchy, "hierarchyDF")
    Redis.closeRedisConnect()
  }

}