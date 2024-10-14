package org.ekstep.analytics.dashboard.report.monthly.requests

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobDriver}

object MonthlyRequestJob extends optional.Application with IJob{
  implicit val className = "org.ekstep.analytics.dashboard..report.monthly.requests.MonthlyRequestJob"

  override def main(config: String)(implicit sc: Option[SparkContext], fc: Option[FrameworkContext]): Unit ={
    implicit val sparkContext: SparkContext = sc.getOrElse(null);
    JobLogger.log("Started executing Monthly Requests Job")
    JobDriver.run("batch", config, MonthlyRequestModel)
    JobLogger.log("Job Completed.")
  }
}
