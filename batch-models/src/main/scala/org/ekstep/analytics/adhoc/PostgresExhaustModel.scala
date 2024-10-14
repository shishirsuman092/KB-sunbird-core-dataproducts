package org.ekstep.analytics.dashboard.exhaust

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig}
import org.ekstep.analytics.framework._
import sys.process._

/**
 * Model for processing dashboard data
 */
object PostgresExhaustModel extends AbsDashboardModel {

  implicit val className: String = "org.ekstep.analytics.dashboard.exhaust.PostgresExhaustModel"
  override def name() = "PostgresExhaustModel"

  /**
   * Master method, does all the work, fetching, processing and dispatching
   *
   * @param timestamp unique timestamp from the start of the processing
   */
  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {

    // org hierarchy
    val dwPostgresUrl = s"jdbc:postgresql://${conf.dwPostgresHost}/${conf.dwPostgresSchema}"
    val orgPostgresDF = postgresTableAsDataFrame(dwPostgresUrl, conf.dwOrgTable, conf.dwPostgresUsername, conf.dwPostgresCredential)
    warehouseCache.write(orgPostgresDF, "orgHierarchy")

    // assessment detail
    val assessmentDetailPostgresDF = postgresTableAsDataFrame(dwPostgresUrl, conf.dwAssessmentTable, conf.dwPostgresUsername, conf.dwPostgresCredential)
    warehouseCache.write(assessmentDetailPostgresDF, "assessmentDetail")

    // bp_enrolments
    val bpEnrolmentsPostgresDF = postgresTableAsDataFrame(dwPostgresUrl, conf.dwBPEnrollmentsTable, conf.dwPostgresUsername, conf.dwPostgresCredential)
    warehouseCache.write(bpEnrolmentsPostgresDF, "bpEnrolments")

    // cb_plan.sh
    val cbPlanPostgresDF = postgresTableAsDataFrame(dwPostgresUrl, conf.dwCBPlanTable, conf.dwPostgresUsername, conf.dwPostgresCredential)
    warehouseCache.write(cbPlanPostgresDF, "cbPlan")

    // content
    val coursePostgresDF = postgresTableAsDataFrame(dwPostgresUrl, conf.dwCourseTable, conf.dwPostgresUsername, conf.dwPostgresCredential)
    warehouseCache.write(coursePostgresDF, "content")

    // content_resource
    val contentResourcePostgresDF = postgresTableAsDataFrame(dwPostgresUrl, conf.dwContentResourceTable, conf.dwPostgresUsername, conf.dwPostgresCredential)
    warehouseCache.write(contentResourcePostgresDF, "contentResource")

    // kcm_content_mapping
    val kcmContentPostgresDF = postgresTableAsDataFrame(dwPostgresUrl, conf.dwKcmContentTable, conf.dwPostgresUsername, conf.dwPostgresCredential)
    warehouseCache.write(kcmContentPostgresDF, "kcmContentMapping")

    // kcm_dictionary
    val kcmDictionaryPostgresDF = postgresTableAsDataFrame(dwPostgresUrl, conf.dwKcmDictionaryTable, conf.dwPostgresUsername, conf.dwPostgresCredential)
    warehouseCache.write(kcmDictionaryPostgresDF, "kcmDictionary")

    // user_detail
    val userDetailPostgresDF = postgresTableAsDataFrame(dwPostgresUrl, conf.dwUserTable, conf.dwPostgresUsername, conf.dwPostgresCredential)
    warehouseCache.write(userDetailPostgresDF, "userDetail")

    // user_enrolments
    val userEnrolmentsPostgresDF = postgresTableAsDataFrame(dwPostgresUrl, conf.dwEnrollmentsTable, conf.dwPostgresUsername, conf.dwPostgresCredential)
    warehouseCache.write(userEnrolmentsPostgresDF, "userEnrolments")

    // root path to bq scripts
    val bqScriptPath = conf.bqScriptPath

    // execute the scripts
    bqScriptPath !;
  }
}

