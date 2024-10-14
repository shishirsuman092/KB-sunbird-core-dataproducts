package org.ekstep.analytics.dashboard.bq

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobDriver}
import org.ekstep.analytics.framework.util.JobLogger

object BqDataJob extends optional.Application with IJob{
  implicit val className = "org.ekstep.analytics.dashboard.bq.BqDataJob"
  override def main(config: String)(implicit sc: Option[SparkContext], fc: Option[FrameworkContext]): Unit ={
    implicit val sparkContext: SparkContext = sc.getOrElse(null);
    JobLogger.log("Started executing Job")
    JobDriver.run("batch", config, BqDataModel)
    JobLogger.log("Job Completed.")
  }
}
