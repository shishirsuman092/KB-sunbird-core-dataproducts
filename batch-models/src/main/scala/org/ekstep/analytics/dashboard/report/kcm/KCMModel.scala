package org.ekstep.analytics.dashboard.report.kcm

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{MapType, StringType}
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig}
import org.ekstep.analytics.framework.FrameworkContext

object KCMModel extends AbsDashboardModel {

  implicit val className: String = "org.ekstep.analytics.dashboard.report.kcm.KCMModel"
  override def name() = "KCMModel"
  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {

    val appPostgresUrl = s"jdbc:postgresql://${conf.appPostgresHost}/${conf.appPostgresSchema}"
    val today = getDate()
    val reportPathContentCompetencyMapping = s"${conf.kcmReportPath}/${today}/ContentCompetencyMapping"
    val reportPathCompetencyHierarchy = s"${conf.kcmReportPath}/${today}/CompetencyHierarchy"
    val fileName = "ContentCompetencyMapping"

    // Content - Competency Mapping data
    val categories = Seq("Course", "Program", "Blended Program", "CuratedCollections", "Standalone Assessment", "Curated Program")
    val cbpDetails = allCourseProgramESDataFrame(categories)
      .where("courseStatus IN ('Live', 'Retired')")
      .select(col("courseID"),col("competencyAreaId"),col("competencyThemeId"),col("competencySubThemeId"), col("courseName"))
    // explode area, theme and sub theme seperately
    val areaExploded = cbpDetails.select(col("courseID"), expr("posexplode_outer(competencyAreaId) as (pos, competency_area_id)"))
    val themeExploded = cbpDetails.select(col("courseID"), expr("posexplode_outer(competencyThemeId) as (pos, competency_theme_id)"))
    val subThemeExploded = cbpDetails.select(col("courseID"), expr("posexplode_outer(competencySubThemeId) as (pos, competency_sub_theme_id)"))
    // Joining area, theme and subtheme based on position
    val competencyJoinedDF = areaExploded.join(themeExploded, Seq("courseID", "pos")).join(subThemeExploded, Seq("courseID", "pos"))
    // joining with cbpDetails for getting courses with no competencies mapped to it
    val competencyContentMappingDF = cbpDetails
      .join(competencyJoinedDF, Seq("courseID"), "left")
      .dropDuplicates(Seq("courseID","competency_area_id","competency_theme_id","competency_sub_theme_id"))
    val contentMappingDF = competencyContentMappingDF.withColumn("data_last_generated_on", currentDateTime)
      .select(col("courseID").alias("course_id"), col("competency_area_id"), col("competency_theme_id"), col("competency_sub_theme_id"), col("data_last_generated_on"))
    show(contentMappingDF, "competency content mapping df")

    generateReport(contentMappingDF.coalesce(1), s"${reportPathContentCompetencyMapping}-warehouse")

    // Competency details data with hierarchy
    val jsonSchema = MapType(StringType, StringType)
    // fetch data from data_node(competency details) and node_mapping(hierarchy)
    val competencyDataDF = postgresTableAsDataFrame(appPostgresUrl, conf.postgresCompetencyTable, conf.appPostgresUsername, conf.appPostgresCredential)
      .select(col("id"), col("name"), col("description"), col("additional_properties"))
      .withColumn("jsonThemeType", from_json(col("additional_properties"), jsonSchema)).drop("additional_properties")
    show(competencyDataDF, "competency data df")
    val competencyMappingDF = postgresTableAsDataFrame(appPostgresUrl, conf.postgresCompetencyHierarchyTable, conf.appPostgresUsername, conf.appPostgresCredential)
      .select(col("id"), col("parent_id"), col("child_id"))
    show(competencyMappingDF, "competency mapping DF")

    // making hierarchy
    val competencyHierarchyDF = competencyMappingDF
      .join(competencyMappingDF
        .withColumnRenamed("parent_id", "parent_parent_id")
        .withColumnRenamed("child_id", "parent_child_id"), col("child_id") === col("parent_parent_id"))
      .select(col("parent_id").alias("competency_area_id"), col("child_id").alias("competency_theme_id"), col("parent_child_id").alias("competency_sub_theme_id"))
    show(competencyHierarchyDF, "competency hierarchy DF")

    // enrich hierarchy with details from data_node
    val competencyDetailsDF = competencyHierarchyDF
      .join(competencyDataDF, col("id")===col("competency_area_id"))
      .withColumnRenamed("name","competency_area").withColumnRenamed("description","competency_area_description").drop("id","jsonThemeType")
      .join(competencyDataDF, col("id")===col("competency_theme_id"))
      .withColumnRenamed("name","competency_theme").withColumnRenamed("description","competency_theme_description").drop("id")
      .withColumn("competency_theme_type", col("jsonThemeType.themeType"))
      .join(competencyDataDF, col("id")===col("competency_sub_theme_id"))
      .withColumnRenamed("name","competency_sub_theme").withColumnRenamed("description","competency_sub_theme_description")
      .withColumn("data_last_generated_on", currentDateTime)
      .select("competency_area_id", "competency_area", "competency_area_description", "competency_theme_type",
        "competency_theme_id", "competency_theme", "competency_theme_description", "competency_sub_theme_id",
        "competency_sub_theme", "competency_sub_theme_description","data_last_generated_on")
    show(competencyDetailsDF, "Competency details dataframe")

    generateReport(competencyDetailsDF.coalesce(1), s"${reportPathCompetencyHierarchy}-warehouse")

    // competency reporting
    val competencyReporting = competencyContentMappingDF.join(competencyDetailsDF, Seq("competency_area_id","competency_theme_id","competency_sub_theme_id"))
      .select(
        col("courseID").alias("content_id"),
        col("courseName").alias("content_name"),
        col("competency_area"),
        col("competency_area_description"),
        col("competency_theme"),
        col("competency_theme_description"),
        col("competency_theme_type"),
        col("competency_sub_theme"),
        col("competency_sub_theme_description")
      ).orderBy("content_id")
    show(competencyReporting, "Competency reporting dataframe")

    generateReport(competencyReporting, reportPathContentCompetencyMapping, fileName=fileName)
    // making report sync configurable
    if (conf.reportSyncEnable) {
      syncReports(s"${conf.localReportDir}/${reportPathContentCompetencyMapping}", reportPathContentCompetencyMapping)
    }

  }
}
