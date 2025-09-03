package org.ekstep.analytics.dashboard.externalContent

import org.apache.spark.SparkContext
import org.ekstep.analytics.dashboard.exhaust.DataExhaustModel
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobDriver}
import org.ekstep.analytics.framework.util.JobLogger

object ExternalContentJob extends optional.Application with IJob {

  implicit val className = "org.ekstep.analytics.dashboard.externalContent.ExternalContentJob"

  def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {
    implicit val sparkContext: SparkContext = sc.getOrElse(null);
    JobLogger.log("Started executing Job")
    JobDriver.run("batch", config, ExternalContentModel);
    JobLogger.log("Job Completed.")
  }
}
