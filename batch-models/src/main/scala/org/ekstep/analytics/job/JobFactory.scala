package org.ekstep.analytics.job

import org.ekstep.analytics.dashboard.DashboardSyncJob
import org.ekstep.analytics.dashboard.exhaust.DataExhaustJob
import org.ekstep.analytics.dashboard.karma.points.KarmaPointsJob
import org.ekstep.analytics.dashboard.leaderboard.LearnerLeaderBoardJob
import org.ekstep.analytics.dashboard.report.kcm.KCMJob
import org.ekstep.analytics.dashboard.report.acbp.UserACBPReportJob
import org.ekstep.analytics.dashboard.report.assess.UserAssessmentJob
import org.ekstep.analytics.dashboard.report.blended.BlendedProgramReportJob
import org.ekstep.analytics.dashboard.report.cba.CourseBasedAssessmentJob
import org.ekstep.analytics.dashboard.report.commsconsole.CommsReportJob
import org.ekstep.analytics.dashboard.report.user.UserReportJob
import org.ekstep.analytics.dashboard.survey.nps.NpsJob
import org.ekstep.analytics.dashboard.report.course.CourseReportJob
import org.ekstep.analytics.dashboard.report.enrolment.UserEnrolmentJob
import org.ekstep.analytics.dashboard.report.observation.question.ObservationQuestionReportJob
import org.ekstep.analytics.dashboard.report.observation.status.ObservationStatusReportJob
import org.ekstep.analytics.dashboard.report.survey.question.QuestionReportJob
import org.ekstep.analytics.dashboard.report.survey.status.StatusReportJob
import org.ekstep.analytics.dashboard.report.warehouse.DataWarehouseJob
import org.ekstep.analytics.dashboard.report.zipreports.ZipReportsWithSecurityJob
import org.ekstep.analytics.dashboard.survey.inappreview.InAppReviewJob
import org.ekstep.analytics.dashboard.telemetry.SummaryRedisSyncJob
import org.ekstep.analytics.dashboard.weekly.claps.WeeklyClapsJob
import org.ekstep.analytics.dashboard.ministrymetrics.MinistryMetricsJob
import org.ekstep.analytics.dashboard.ministryleaderboard.MinistryLearnerLeaderboardJob

import scala.reflect.runtime.universe
import org.ekstep.analytics.framework.IJob
import org.ekstep.analytics.framework.exception.JobNotFoundException
import org.ekstep.analytics.job.batch.VideoStreamingJob
import org.ekstep.analytics.job.metrics.MetricsAuditJob
import org.ekstep.analytics.job.summarizer.DruidQueryProcessor
import org.ekstep.analytics.job.summarizer.ExperimentDefinitionJob
import org.ekstep.analytics.job.summarizer.MonitorSummarizer
import org.ekstep.analytics.job.summarizer.WorkFlowSummarizer
import org.ekstep.analytics.job.updater.ContentRatingUpdater
import org.ekstep.analytics.exhaust.OnDemandDruidExhaustJob

/**
 * @author Santhosh
 */

object JobFactory {
  @throws(classOf[JobNotFoundException])
  def getJob(jobType: String): IJob = {
    jobType.toLowerCase() match {
      case "monitor-job-summ" =>
        MonitorSummarizer
      case "wfs" =>
        WorkFlowSummarizer
      case "data-exhaust" =>
        DataExhaustJob
      case "dashboard-sync" =>
        DashboardSyncJob
      case "assessment-metrics" =>
        UserAssessmentJob
      case "summary-redis-sync" =>
        SummaryRedisSyncJob
      case "user-report" =>
        UserReportJob
      case "user-enrolment-report" =>
        UserEnrolmentJob
      case "course-report" =>
        CourseReportJob
      case "course-based-assessment-report" =>
        CourseBasedAssessmentJob
      case "blended-program-report" =>
        BlendedProgramReportJob
      case "data-warehouse-sync" =>
        DataWarehouseJob
      case "weekly-claps" =>
        WeeklyClapsJob
      case "comms-console" =>
        CommsReportJob
      case "karma-points" =>
        KarmaPointsJob
      case "learner-leaderboard" =>
        LearnerLeaderBoardJob
      case "kcm-report" =>
        KCMJob
      case "acbp-report" =>
        UserACBPReportJob
      case "video-streaming" =>
        VideoStreamingJob
      case "telemetry-replay" =>
        EventsReplayJob
      case "summary-replay" =>
        EventsReplayJob
      case "content-rating-updater" =>
        ContentRatingUpdater
      case "experiment" =>
        ExperimentDefinitionJob
      case "audit-metrics-report" =>
        MetricsAuditJob
      case "druid_reports" =>
        DruidQueryProcessor
      case "druid-dataset" =>
        OnDemandDruidExhaustJob
      case "survey-nps" =>
        NpsJob
      case "in-app-review-feed" =>
        InAppReviewJob
      case "zip-upload" =>
        ZipReportsWithSecurityJob
      case "survey-question-report" =>
        QuestionReportJob
      case "survey-status-report" =>
        StatusReportJob
      case "observation-question-report" =>
        ObservationQuestionReportJob
      case "observation-status-report" =>
        ObservationStatusReportJob
      case "ministry-metrics" =>
        MinistryMetricsJob
      case "ministry-learner-leaderboard" =>
        MinistryLearnerLeaderboardJob
      case _ =>
        reflectModule(jobType);
    }
  }

  def reflectModule(jobClass: String): IJob = {
    try {
      val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
      val module = runtimeMirror.staticModule(jobClass)
      val obj = runtimeMirror.reflectModule(module)
      obj.instance.asInstanceOf[IJob]
    } catch {
      case ex: Exception =>
        throw new JobNotFoundException("Unknown job type found")
    }
  }

}
