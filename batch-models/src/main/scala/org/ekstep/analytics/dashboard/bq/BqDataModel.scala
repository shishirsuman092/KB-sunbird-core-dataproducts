package org.ekstep.analytics.dashboard.bq

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig}
import org.ekstep.analytics.framework.FrameworkContext
import sys.process._

object BqDataModel extends AbsDashboardModel {
  override def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
    // root path to bq scripts
    val bqScriptPath = conf.bqScriptPath

    // execute the scripts
    bqScriptPath!;
  }
}
