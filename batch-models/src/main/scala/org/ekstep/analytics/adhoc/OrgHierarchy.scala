package org.ekstep.analytics.adhoc

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit, to_timestamp, when}
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig}
import org.ekstep.analytics.framework.FrameworkContext

object OrgHierarchy extends AbsDashboardModel {

  implicit val className: String = "org.ekstep.analytics.adhoc.OrgHierarchy"

  override def name() = "OrgHierarchy"

  /**
   * Master method, does all the work, fetching, processing and dispatching
   *
   * @param timestamp unique timestamp from the start of the processing
   */
  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    val org = cache.load("org")
    val org_hierarchy = cache.load("orgHierarchy")
    val org_complete_hierarchy = cache.load("orgCompleteHierarchy")

    show(org)
    show(org_hierarchy)
    show(org_complete_hierarchy)

    val orgCassandraDF = org
      .withColumn("createddate", to_timestamp(col("createddate"), "yyyy-MM-dd HH:mm:ss:SSSZ"))
      .select(
        col("id").alias("sborgid"),
        col("organisationtype").alias("orgType"),
        col("orgname").alias("cassOrgName"),
        col("createddate").alias("orgCreatedDate"),
        col("createdby").alias("createdBy")
      )
    val orgDfWithOrgType = orgCassandraDF.join(org_complete_hierarchy, Seq("sborgid"), "left")

    show(orgDfWithOrgType)
    val orgHierarchyDF = orgDfWithOrgType
      .select(
        col("sborgid").alias("mdo_id"),
        col("cassOrgName").alias("mdo_name"),
        col("l1orgname").alias("ministry"),
        col("l2orgname").alias("department"),
        col("orgCreatedDate").alias("mdo_created_on"),
        col("orgType"),
        col("createdBy"),
        col("l1mapid"),
        col("l2mapid")
      )
      .withColumn("is_content_provider",
        when(col("orgType").cast("int") === 128 || col("orgType").cast("int") === 128, lit("Y")).otherwise(lit("N")))
      .withColumn("organization", when(col("ministry").isNotNull && col("department").isNotNull, col("mdo_name")).otherwise(null))
      .withColumn("data_last_generated_on", currentDateTime)
      .distinct()
      .drop("orgType")
      .dropDuplicates(Seq("mdo_id"))

    val org2 = orgHierarchyDF.join(orgDfWithOrgType, orgHierarchyDF("l1mapid") === orgDfWithOrgType("mapid"), "left")
      .select(
      col("mdo_id"),
      col("mdo_name"),
      col("ministry"),
      col("department"),
      col("mdo_created_on"),
      col("orgType"),
      col("createdBy"),
      col("sborgid").alias("l1ID"),
      col("l1mapid"),
      col("l2mapid")
      )

    val finalORG = org2.join(orgDfWithOrgType, orgHierarchyDF("l2mapid") === orgDfWithOrgType("mapid"), "left")
      .select(
        col("mdo_id"),
        col("mdo_name"),
        col("ministry"),
        col("department"),
        col("mdo_created_on"),
        col("orgType"),
        col("createdBy"),
        col("l1ID"),
        col("l1mapid"),
        col("sborgid").alias("l2ID"),
        col("l2mapid")
      )

    show(finalORG)
    
  }
}

