package org.ekstep.analytics.dashboard.bq

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.ekstep.analytics.dashboard.DashboardUtil.getDate
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig}
import org.ekstep.analytics.framework.FrameworkContext
import sys.process._

object BqDataModel extends AbsDashboardModel {
  override def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    // today's date
    val today = getDate()
    // root path to bq scripts
    val baseScriptPath = conf.bqScriptPath
    // script file path
    val assessmentScriptFilePath = baseScriptPath.concat("/").concat(conf.assessmentScriptFileName)
    val bpEnrolmentsScriptFilePath = baseScriptPath.concat("/").concat(conf.bpEnrolmentsScriptFileName)
    val cbPlanScriptFilePath = baseScriptPath.concat("/").concat(conf.cbPlanScriptFileName)
    val contentScriptFilePath = baseScriptPath.concat("/").concat(conf.contentScriptFileName)
    val contentResourceScriptFilePath = baseScriptPath.concat("/").concat(conf.contentResourceScriptFileName)
    val kcmContentMappingScriptFilePath = baseScriptPath.concat("/").concat(conf.kcmContentMappingScriptFileName)
    val kcmDictionaryScriptFilePath = baseScriptPath.concat("/").concat(conf.kcmDictionaryScriptFileName)
    val orgHierarchyScriptFilePath = baseScriptPath.concat("/").concat(conf.orgHierarchyScriptFileName)
    val userDetailScriptFilePath = baseScriptPath.concat("/").concat(conf.userDetailScriptFileName)
    val userEnrolmentsScriptFilePath = baseScriptPath.concat("/").concat(conf.userEnrolmentsScriptFileName)

    // execute the scripts
    assessmentScriptFilePath!;
    bpEnrolmentsScriptFilePath!;
    cbPlanScriptFilePath!;
    contentScriptFilePath!;
    contentResourceScriptFilePath!;
    kcmContentMappingScriptFilePath!;
    kcmDictionaryScriptFilePath!;
    orgHierarchyScriptFilePath!;
    userDetailScriptFilePath!;
    userEnrolmentsScriptFilePath!;
  }
}
