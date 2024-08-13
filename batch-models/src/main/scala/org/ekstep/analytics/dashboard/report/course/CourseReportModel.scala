package org.ekstep.analytics.dashboard.report.course

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig, Redis}
import org.ekstep.analytics.framework.FrameworkContext


object CourseReportModel extends AbsDashboardModel {
    implicit val className: String = "org.ekstep.analytics.dashboard.report.course.CourseReportModel"

    override def name() = "CourseReportModel"

    def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
        val today = getDate()

        val orgDF = orgDataFrame()
        // Get course data first
        val allCourseProgramDetailsDF = contentWithOrgDetailsDataFrame(orgDF, Seq("Course", "Program", "Blended Program", "CuratedCollections", "Curated Program"))
        // Get content resource hierarchy
        var contentResourceHierarchyDF = contentHierarchyDataFrame()
        // Rename the identifier field to courseID in order to perform join
        contentResourceHierarchyDF = contentResourceHierarchyDF.withColumnRenamed("identifier", "courseID")
        // Perform join on contentResourceHierarchy and allCourseProgramDetails
        val getContentResourceWithCategoryDF = contentResourceHierarchyDF
          .join(allCourseProgramDetailsDF, Seq("courseID"), "inner")
          .select(contentResourceHierarchyDF.columns.map(col) ++ Seq(allCourseProgramDetailsDF("category")): _*)

        // Define the schema for the hierarchy column
        val hierarchySchema = StructType(Seq(
            StructField("children", ArrayType(StructType(Seq(
                StructField("identifier", StringType),
                StructField("primaryCategory", StringType),
                StructField("name", StringType),
                StructField("duration", StringType),
                StructField("expectedDuration", StringType),
                StructField("children", ArrayType(StructType(Seq(
                    StructField("identifier", StringType),
                    StructField("primaryCategory", StringType),
                    StructField("name", StringType),
                    StructField("duration", StringType),
                    StructField("expectedDuration", StringType)
                ))))
            ))))
        ))

        // Filter getContentResourceWithCategoryDF based on categories "Program", "Curated Program", and "Course"
        val filteredDF = getContentResourceWithCategoryDF
          .filter(col("category").isin("Program", "Curated Program", "Course"))

        // Parse the hierarchy column as a struct using from_json
        val parsedDF = filteredDF.withColumn("hierarchy", from_json(col("hierarchy"), hierarchySchema))

        // Explode the hierarchy column to get the first-level children
        val firstLevelDF = parsedDF
          .select(
              col("courseID").alias("content_id"),
              col("category"),
              explode_outer(col("hierarchy.children")).as("first_level_child")
          )

        // Explode the first-level children to get the second-level children
        var resultDF = firstLevelDF
          .withColumn("second_level_child", explode_outer(col("first_level_child.children")))
          .select(
              col("content_id"),
              when(col("category").isin("Program", "Curated Program"), col("first_level_child.identifier"))
                .otherwise(
                    when(col("first_level_child.primaryCategory") === "Course Unit", col("second_level_child.identifier"))
                      .otherwise(col("first_level_child.identifier"))
                ).alias("resource_id"),
              when(col("category").isin("Program", "Curated Program"), col("first_level_child.name"))
                .otherwise(
                    when(col("first_level_child.primaryCategory") === "Course Unit", col("second_level_child.name"))
                      .otherwise(col("first_level_child.name"))
                ).alias("resource_name"),
              when(col("category").isin("Program", "Curated Program"), col("first_level_child.primaryCategory"))
                .otherwise(
                    when(col("first_level_child.primaryCategory") === "Course Unit", col("second_level_child.primaryCategory"))
                      .otherwise(col("first_level_child.primaryCategory"))
                ).alias("resource_type"),
              when(col("category").isin("Program", "Curated Program"), col("first_level_child.duration"))
                .otherwise(
                    when(col("first_level_child.primaryCategory") === "Course Unit",
                        coalesce(col("second_level_child.duration"), col("second_level_child.expectedDuration")))
                      .otherwise(coalesce(col("first_level_child.duration"), col("first_level_child.expectedDuration")))
                ).alias("resource_duration")
          )

        var result1DF = resultDF.durationFormat("resource_duration")
        val notNullDF = result1DF.filter(col("resource_id").isNotNull && col("resource_id") =!= "")
        // Remove completely identical rows, keeping only one
        val distinctDF = notNullDF.dropDuplicates(notNullDF.columns).withColumn("data_last_generated_on", currentDateTime)
        // Show the result DataFrame
        distinctDF.show()
        val reportPath = s"${conf.courseReportPath}/${today}"
        generateReport(distinctDF.coalesce(1), s"${reportPath}-resource-warehouse")

            val userRatingDF = userCourseRatingDataframe().groupBy("courseID").agg(
              avg(col("userRating")).alias("rating")
            )
            val cbpDetailsDF = allCourseProgramDetailsDF.join(userRatingDF, Seq("courseID"), "left")
            show(cbpDetailsDF, "cbpDetailsDataFrame")

            val courseResCountDF = allCourseProgramDetailsDF.select("courseID", "courseResourceCount")
            val userEnrolmentDF = userCourseProgramCompletionDataFrame().join(courseResCountDF, Seq("courseID"), "left")
            val allCBPCompletionWithDetailsDF = calculateCourseProgress(userEnrolmentDF)
            show(allCBPCompletionWithDetailsDF, "allCBPCompletionWithDetailsDF")

            val aggregatedDF = allCBPCompletionWithDetailsDF.groupBy("courseID")
              .agg(
                min("courseCompletedTimestamp").alias("earliestCourseCompleted"),
                max("courseCompletedTimestamp").alias("latestCourseCompleted"),
                count("*").alias("enrolledUserCount"),
                sum(when(col("userCourseCompletionStatus") === "in-progress", 1).otherwise(0)).alias("inProgressCount"),
                sum(when(col("userCourseCompletionStatus") === "not-started", 1).otherwise(0)).alias("notStartedCount"),
                sum(when(col("userCourseCompletionStatus") === "completed", 1).otherwise(0)).alias("completedCount"),
                  sum(col("issuedCertificateCountPerContent")).alias("totalCertificatesIssued")
              )
              .withColumn("firstCompletedOn", to_date(col("earliestCourseCompleted"), "dd/MM/yyyy"))
              .withColumn("lastCompletedOn", to_date(col("latestCourseCompleted"), "dd/MM/yyyy"))
            show(aggregatedDF, "aggregatedDF")

            val allCBPAndAggDF = cbpDetailsDF.join(aggregatedDF, Seq("courseID"), "left")
            show(allCBPAndAggDF, "allCBPAndAggDF")

            val courseBatchDF = courseBatchDataFrame()
            val relevantBatchInfoDF = allCourseProgramDetailsDF.select("courseID", "category")
              .where(expr("category IN ('Blended Program')"))
              .join(courseBatchDF, Seq("courseID"), "left")
              .select("courseID", "batchID", "courseBatchName", "courseBatchStartDate", "courseBatchEndDate")
            show(relevantBatchInfoDF, "relevantBatchInfoDF")

            // val curatedCourseDataDFWithBatchInfo = allCBPAndAggDF.join(relevantBatchInfoDF, Seq("courseID"), "left")
            val curatedCourseDataDFWithBatchInfo = allCBPAndAggDF
              .coalesce(1) // gives OOM without this
              .join(relevantBatchInfoDF, Seq("courseID"), "left")
            show(curatedCourseDataDFWithBatchInfo, "curatedCourseDataDFWithBatchInfo")

            val fullDF = curatedCourseDataDFWithBatchInfo
              .where(expr("courseStatus IN ('Live', 'Draft', 'Retired', 'Review')"))
              .durationFormat("courseDuration")
              .withColumn("courseLastPublishedOn", to_date(col("courseLastPublishedOn"), "dd/MM/yyyy"))
              .withColumn("courseBatchStartDate", to_date(col("courseBatchStartDate"), "dd/MM/yyyy"))
              .withColumn("courseBatchEndDate", to_date(col("courseBatchEndDate"), "dd/MM/yyyy"))
              .withColumn("lastStatusChangedOn", to_date(col("lastStatusChangedOn"), "dd/MM/yyyy"))
              .withColumn("ArchivedOn", when(col("courseStatus").equalTo("Retired"), to_date(col("lastStatusChangedOn"), "dd/MM/yyyy")))
              .withColumn("Report_Last_Generated_On", date_format(current_timestamp(), "dd/MM/yyyy HH:mm:ss a"))

            val fullReportDF = fullDF
              .select(
                col("courseID"),
                col("courseOrgID"),
                col("courseStatus").alias("Content_Status"),
                col("courseOrgName").alias("Content_Provider"),
                col("courseName").alias("Content_Name"),
                col("category").alias("Content_Type"),
                col("batchID").alias("Batch_Id"),
                col("courseBatchName").alias("Batch_Name"),
                col("courseBatchStartDate").alias("Batch_Start_Date"),
                col("courseBatchEndDate").alias("Batch_End_Date"),
                col("courseDuration").alias("Content_Duration"),
                col("enrolledUserCount").alias("Enrolled"),
                col("notStartedCount").alias("Not_Started"),
                col("inProgressCount").alias("In_Progress"),
                col("completedCount").alias("Completed"),
                col("rating").alias("Content_Rating"),
                col("courseLastPublishedOn").alias("Last_Published_On"),
                col("firstCompletedOn").alias("First_Completed_On"),
                col("lastCompletedOn").alias("Last_Completed_On"),
                col("ArchivedOn").alias("Content_Retired_On"),
                col("totalCertificatesIssued").alias("Total_Certificates_Issued"),
                col("courseOrgID").alias("mdoid"),
                col("Report_Last_Generated_On")
              )
              .coalesce(1)
            show(fullReportDF, "fullReportDF")

            // generateReport(fullReportDF, s"${reportPath}-full")
            val mdoReportDF = fullReportDF.drop("courseID", "courseOrgID")
            generateReport(mdoReportDF,  reportPath,"mdoid", "ContentReport")
            // to be removed once new security job is created
            if (conf.reportSyncEnable) {
              syncReports(s"${conf.localReportDir}/${reportPath}", reportPath)
            }

            val df_warehouse = fullDF
              .withColumn("data_last_generated_on", currentDateTime)
              .select(
                col("courseID").alias("content_id"),
                col("courseOrgID").alias("content_provider_id"),
                col("courseOrgName").alias("content_provider_name"),
                col("courseName").alias("content_name"),
                col("category").alias("content_type"),
                col("batchID").alias("batch_id"),
                col("courseBatchName").alias("batch_name"),
                col("courseBatchStartDate").alias("batch_start_date"),
                col("courseBatchEndDate").alias("batch_end_date"),
                col("courseDuration").alias("content_duration"),
                col("rating").alias("content_rating"),
                date_format(col("courseLastPublishedOn"), "yyyy-MM-dd").alias("last_published_on"),
                col("ArchivedOn").alias("content_retired_on"),
                col("courseStatus").alias("content_status"),
                col("courseResourceCount").alias("resource_count"),
                col("totalCertificatesIssued").alias("total_certificates_issued"),
                col("courseReviewStatus").alias("content_substatus"),
                col("contentLanguage").alias("language"),
                col("data_last_generated_on")
              )
            generateReport(df_warehouse.coalesce(1), s"${reportPath}-warehouse")

        Redis.closeRedisConnect()
    }

}