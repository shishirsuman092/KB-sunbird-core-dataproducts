package org.ekstep.analytics.dashboard.report.observation.status

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig}
import org.ekstep.analytics.framework.FrameworkContext
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.text.SimpleDateFormat
import scala.collection.JavaConverters._

object ObservationStatusReportModel extends AbsDashboardModel {

  implicit val className: String = "org.ekstep.analytics.dashboard.report.observation.status.ObservationStatusReportModel"

  override def name() = "ObservationStatusReportModel"

  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {

    val today = getDate()
    println("Querying mongo database to get report configurations")
    val observationStatusReportColumnsConfig = getReportConfig("observationStatusReport")
    val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
    val observationStatusReportColumnsConfigMap = mapper.readValue(observationStatusReportColumnsConfig, classOf[Map[String, String]])
    val reportColumnsMap = observationStatusReportColumnsConfigMap("reportColumns").asInstanceOf[Map[String, String]]
    val userProfileColumnsMap = observationStatusReportColumnsConfigMap("userProfileColumns").asInstanceOf[Map[String, String]]
    val sortingColumns = observationStatusReportColumnsConfigMap("sortingColumns")
    val columnsToBeQueried = reportColumnsMap.keys.mkString(",") //+ ",userProfile"
    val userProfileSchema = StructType(userProfileColumnsMap.keys.toSeq.map(key => StructField(key, StringType, nullable = true)))
    val reportColumns = reportColumnsMap.keys.toList.map(key => col(key).as(reportColumnsMap(key)))
    val userProfileColumns = userProfileColumnsMap.keys.toList.map(key => col(s"parsedProfile.$key").as(userProfileColumnsMap(key)))
    val requiredCsvColumns = reportColumns ++ userProfileColumns
    val reportPath = s"${conf.mlReportPath}/${today}/ObservationCompletedSubmissionsReport" 

    /**
     * Check to see if there is any solutionId are passed from config if Yes generate report only for those ID's
     * If not generate report for all unique solutionId's from druid sl-observation-meta datasource.
     */
    val solutionIds = conf.solutionIDs
    if (solutionIds != null && solutionIds.trim.nonEmpty) {
      println("Processing report requests from the configurations")
      val solutionIdsDF = getSolutionIdsAsDF(solutionIds)

      solutionIdsDF.collect().foreach { row =>
        val solutionId = row.getString(0)
        val solutionName = row.getString(1)
        println(s"Started processing report request for solutionId: $solutionId")
        generateObservationStatusReport(solutionId, solutionName)
      }
    } else {
      println("Processing report requests for all solutionId's")
      println("Querying druid to get all the unique solutionId's")
      val solutionIdsDF = loadAllUniqueSolutionIds("sl-observation-meta")

      if (conf.includeExpiredSolutionIDs) {
        println("Generating report for all the expired solutionId's also")
        solutionIdsDF.collect().foreach { row =>
          val solutionId = row.getString(0)
          val solutionName = row.getString(1)
          println(s"Started processing report request for solutionId: $solutionId")
          generateObservationStatusReport(solutionId, solutionName)
        }
      } else {
        println("Query mongodb to get solution end-date for all the unique solutionId's")
        val solutionsEndDateDF = getSolutionsEndDate(solutionIdsDF)
        solutionsEndDateDF.collect().foreach { row =>
          val solutionId = row.getString(0)
          val solutionName = row.getString(1)
          val endDate = new SimpleDateFormat("yyyy-MM-dd").format(row.getDate(1))
          if (endDate != null) {
            println(s"Started processing report request for solutionId: $solutionId")
            if (isSolutionWithinReportDate(endDate)) {
              println(s"Solution with Id $solutionId will ends on $endDate")
              generateObservationStatusReport(solutionId, solutionName)
            } else {
              println(s"Solution with Id $solutionId has ended on $endDate date, Hence not generating the report for this ID ")
            }
          } else {
            println(s"End Date for solutionId: $solutionId is NULL, Hence skipping generating the report for this ID ")
          }
        }
      }

      /**
       * This method takes the endDate and checks if that date is within the Report Date
       *
       * @param endDate
       * @return
       */
      def isSolutionWithinReportDate(endDate: String): Boolean = {
        val formatter = DateTimeFormat.forPattern("yyyy-MM-dd")
        val today = LocalDate.now()
        val updatedDate = today.minusDays(conf.gracePeriod.toInt)
        val endDateOfSolution = formatter.parseLocalDate(endDate)
        endDateOfSolution.isEqual(today) || (endDateOfSolution.isAfter(today) || endDateOfSolution.isAfter(updatedDate)) || endDateOfSolution.isEqual(updatedDate)
      }
    }
    println("Zipping the csv content folder and syncing to blob storage")
    zipAndSyncReports(s"${conf.localReportDir}/${reportPath}", reportPath)
    println("Successfully zipped folder and synced to blob storage")

    /**
     * This method takes solutionId to query, parse userProfile JSON, append status data and sort the CSV
     *
     * @param solutionId
     */
    def generateObservationStatusReport(solutionId: String, solutionName: String) = {
      val dataSource = "sl-observation-meta"
      val batchSize = conf.ObservationStatusReportBatchSize.toInt
      getSolutionIdData(columnsToBeQueried, dataSource, solutionId, solutionName, batchSize)
      println(s"-------------- Successfully Generated Observation Status CSV In Batches And Combined Into A Single File For A SolutionId: ${solutionId} --------------")
    }

    def getSolutionIdData(columns: String, dataSource: String, solutionId: String, solutionName: String, batchSize: Int)(implicit spark: SparkSession, conf: DashboardConfig) {
      import spark.implicits._
      val observationSubmissionIdQuery = raw"""SELECT DISTINCT(observationSubmissionId) FROM \"$dataSource\" WHERE solutionId='$solutionId'"""
      val observationSubmissionId = druidDFOption(observationSubmissionIdQuery, conf.mlSparkDruidRouterHost, limit = 1000000)
        .getOrElse(spark.emptyDataFrame)
        .as[String]
        .collect()
      println(s"Total ${observationSubmissionId.length} Observation Submissions for a solutionId: $solutionId")
      var batchCount = 0
      observationSubmissionId.grouped(batchSize).toList.foreach { batchObservationSubmissionIds =>
        batchCount += 1
        val batchQuery = raw"""SELECT $columns FROM \"$dataSource\" WHERE solutionId='$solutionId' AND observationSubmissionId IN ('${batchObservationSubmissionIds.mkString("','")}')"""
        var batchDF = druidDFOption(batchQuery, conf.mlSparkDruidRouterHost, limit = 1000000).getOrElse(spark.emptyDataFrame)
        batchDF.persist(StorageLevel.DISK_ONLY)
        if (batchDF.columns.contains("evidences")) {
          val baseUrl = conf.baseUrlForEvidences
          val addBaseUrl = udf((evidences: String) => {
            if (evidences != null && evidences.trim.nonEmpty) {
              evidences.split(", ")
                .map(url => s"$baseUrl$url")
                .mkString(",")
            } else {
              evidences
            }
          })
          batchDF = batchDF.withColumn("evidences", addBaseUrl(col("evidences")))
        }
        val initialSolutionDf = processProfileData(batchDF, userProfileSchema, requiredCsvColumns)
        val finalSolutionDf = appendObservationStatusData(initialSolutionDf)
        val columnsMatch = validateColumns(finalSolutionDf, sortingColumns.split(",").map(_.trim))

        if (columnsMatch == true) {
          val columnsOrder = sortingColumns.split(",").map(_.trim)
          val sortedFinalDF = finalSolutionDf.select(columnsOrder.map(col): _*)
          generateReport(sortedFinalDF, reportPath, fileSaveMode = SaveMode.Append)
          println(s"Batch : $batchCount, Successfully generated Observation Status csv report for solutionId: $solutionId")
        } else {
          println(s"Error occurred while matching the data frame columns with config sort columns for solutionId: $solutionId")
        }
        batchDF.unpersist()
      }
      val solutionsName = solutionName
        .replaceAll("[^a-zA-Z0-9\\s]", "")
        .replaceAll("\\s+", " ")
        .trim()
      println(s"Total $batchCount batches processed for solutionId: $solutionId")
      combineCsvFiles(s"${conf.localReportDir}/${reportPath}", s"${conf.localReportDir}/${reportPath}/${solutionsName}-${solutionId}.csv")
    }

    def processProfileData(originalDf: DataFrame, profileSchema: StructType, requiredCsvColumns: List[Column])(implicit spark: SparkSession, conf: DashboardConfig): DataFrame = {
      val hasUserProfileColumn = originalDf.columns.contains("userProfile")
      if (hasUserProfileColumn) {
        val parsedDf = originalDf.withColumn("parsedProfile", from_json(col("userProfile"), profileSchema))
        parsedDf.select(requiredCsvColumns: _*)
      } else {
        val emptyParsedDf = originalDf.withColumn("parsedProfile", lit(null).cast(StringType))
        emptyParsedDf.select(requiredCsvColumns: _*)
      }
    }

    def combineCsvFiles(inputPath: String, outputPath: String): Unit = {
      val inputDir = Paths.get(inputPath)
      val outputFilePath = Paths.get(outputPath)
      val outputStream = Files.newBufferedWriter(outputFilePath, StandardCharsets.UTF_8)
      var isFirstFile = true

      try {
        val partFiles = Files.list(inputDir).iterator().asScala
          .filter(file => file.getFileName.toString.startsWith("part-") && file.getFileName.toString.endsWith(".csv"))
          .toList

        partFiles.foreach { file =>
          val lines = Files.readAllLines(file, StandardCharsets.UTF_8).asScala
          lines.zipWithIndex.foreach { case (line, idx) =>
            if (isFirstFile || idx > 0) {
              outputStream.write(line)
              outputStream.newLine()
            }
          }
          isFirstFile = false
        }
        // Delete part files
        partFiles.foreach(file => Files.delete(file))
      } finally {
        outputStream.close()
      }
    }

    def appendObservationStatusData(solutionDf: DataFrame): DataFrame = {
      println("Processing for observation completed status")
      val completedStatus = getObservationStatusCompletedData(solutionDf)
      val filteredCompletedStatus = completedStatus.filter(col("Status of Submission").isNull || col("Status of Submission") === "")
        .select("Observation Submission Id", "Status of Submission")

      if (!filteredCompletedStatus.isEmpty) {
        println("Processing for observation inprogress status")
        val inProgressStatus = getObservationStatusInProgressData(filteredCompletedStatus)
        val joinedCompletedAndInProgressDF = completedStatus
          .join(inProgressStatus, Seq("Observation Submission Id"), "left")
          .select(
            completedStatus("*"),
            coalesce(inProgressStatus("Status of Submission"), completedStatus("Status of Submission")).as("Updated Status of Submission")
          ).drop("Status of Submission")
          .withColumnRenamed("Updated Status of Submission", "Status of Submission")
        val filteredInProgressStatus = joinedCompletedAndInProgressDF.filter(col("Status of Submission").isNull || col("Status of Submission") === "")
          .select("Observation Submission Id", "Status of Submission")

        if (!filteredInProgressStatus.isEmpty) {
          println("Processing for observation started status")
          val joinedStartedAndInProgressDF = joinedCompletedAndInProgressDF.na.fill("Started", Seq("Status of Submission"))
          return joinedStartedAndInProgressDF
        } else {
          return joinedCompletedAndInProgressDF
        }
      }
      completedStatus
    }

  }

}
