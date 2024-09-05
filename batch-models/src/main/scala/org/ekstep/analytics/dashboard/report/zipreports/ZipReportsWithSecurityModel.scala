package org.ekstep.analytics.dashboard.report.zipreports

import net.lingala.zip4j.ZipFile
import net.lingala.zip4j.model.ZipParameters
import net.lingala.zip4j.model.enums.{CompressionMethod, EncryptionMethod}
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig}
import org.ekstep.analytics.framework._

import java.io.File
import java.nio.file.{Files, Paths, StandardCopyOption}

/**
 * Model for processing the operational reports into a single password protected zip file
 */
object ZipReportsWithSecurityModel extends AbsDashboardModel {

  implicit val className: String = "org.ekstep.analytics.dashboard.report.zipreports.ZipReportsWithSecurityModel"
  override def name() = "ZipReportsWithSecurityModel"

  /**
   * Master method, does all the work, fetching, processing and dispatching
   *
   * @param timestamp unique timestamp from the start of the processing
   */
  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {

    // Fetch the org hierarchy
    val (orgNamesDF, userDF, userOrgDF) = getOrgUserDataFrames()
    // Broadcast smaller DataFrame to avoid shuffling
    val broadcastOrgNamesDF = broadcast(orgNamesDF)

    // Cache frequently used DataFrames
    val cachedUserOrgDF = userOrgDF.persist(StorageLevel.MEMORY_AND_DISK)

    val orgHierarchyDF = getDetailedHierarchy(cachedUserOrgDF)
    // Define directories and date
    val prefixDirectoryPath = s"${conf.localReportDir}/${conf.prefixDirectoryPath}"
    val destinationPath = s"${conf.localReportDir}/${conf.destinationDirectoryPath}"
    val directoriesToSelect = conf.directoriesToSelect.split(",").toSet
    val specificDate = getDate()
    val kcmFolderPath = s"${conf.localReportDir}/${conf.kcmReportPath}/${specificDate}/ContentCompetencyMapping"

    // Optimized method to fetch orgName
    def getOrgName(orgID: String, orgDF: DataFrame): String = {
      val resultDF =  orgDF.filter(col("orgID") === orgID).select("orgName")
      if (resultDF.isEmpty) {
        // Return a default value if no rows are found
        "NotFound"
      } else {
        resultDF.first().getString(0)
      }
    }

    // Method to traverse all the report folders within the source folder
    def traverseDirectory(directory: File): Unit = {
      val files = Option(directory.listFiles()).getOrElse(Array.empty).filter(file => directoriesToSelect.contains(file.getName))
      files.foreach { file =>
        if (file.isDirectory) {
          val dateFolder = new File(file, specificDate)
          if (dateFolder.exists() && dateFolder.isDirectory) traverseDateFolder(dateFolder)
        }
      }
    }

    // Optimized method to traverse and copy CSV files
    def traverseDateFolder(dateFolder: File): Unit = {
      val files = Option(dateFolder.listFiles()).getOrElse(Array.empty)
      files.filter(_.isDirectory).foreach { mdoidFolder =>
        copyFiles(mdoidFolder, destinationPath)
        copyKCMFile(mdoidFolder)
      }
    }

    // Copy all CSV files to the destination directory
    def copyFiles(mdoidFolder: File, destinationPath: String): Unit = {
      val destinationDirectory = Paths.get(destinationPath, mdoidFolder.getName)
      if (!Files.exists(destinationDirectory)) Files.createDirectories(destinationDirectory)
      val csvFiles = Option(mdoidFolder.listFiles()).getOrElse(Array.empty).filter(_.getName.endsWith(".csv"))
      csvFiles.foreach { csvFile =>
        Files.copy(csvFile.toPath, Paths.get(destinationDirectory.toString, csvFile.getName), StandardCopyOption.REPLACE_EXISTING)
      }
    }

    // Copy KCM file
    def copyKCMFile(mdoidFolder: File): Unit = {
      val kcmFile = new File(kcmFolderPath, "ContentCompetencyMapping.csv")
      val destinationDirectory = Paths.get(destinationPath, mdoidFolder.getName)
      if (kcmFile.exists() && destinationDirectory.toFile.exists()) {
        Files.copy(kcmFile.toPath, Paths.get(destinationDirectory.toString, kcmFile.getName), StandardCopyOption.REPLACE_EXISTING)
      }
    }

    // Start traversing the source directory
    traverseDirectory(new File(prefixDirectoryPath))

    // Traverse through destination directory to create individual zip files (mdo-wise)
    val mdoidFolders = Option(new File(destinationPath).listFiles()).getOrElse(Array.empty).filter(_.getName.startsWith("mdoid=0"))
    mdoidFolders.foreach { mdoidFolder =>
      if (mdoidFolder.isDirectory) {
        val orgID = mdoidFolder.getName.split("=")(1)
        val orgFileName = getOrgName(orgID, broadcastOrgNamesDF)
        val sanitizedOrgFileName = orgFileName.replaceAll("[/\\\\]", "_")
        val zipFilePath = s"${mdoidFolder}"

        // Create password-protected zip file
        val zipFile = new ZipFile(zipFilePath + "/" + sanitizedOrgFileName + ".zip")
        val parameters = new ZipParameters()
        parameters.setCompressionMethod(CompressionMethod.DEFLATE)
        mdoidFolder.listFiles().foreach { file =>
          zipFile.addFile(file, parameters)
        }
        // Remove CSVs after zipping
        mdoidFolder.listFiles().foreach { file =>
          if (file.getName.toLowerCase.endsWith(".csv")) file.delete()
        }
        println(s"Individual ZIP file created for $orgID: $zipFilePath")
      } else {
        println("No mdoid folders found in the given directory.")
    }

    // Merging zip files based on hierarchy
    def mergeMdoidFolders(orgHierarchy: DataFrame, baseDir: String): Unit = {
      orgHierarchy.collect().foreach { row =>
        val ministryID = row.getAs[String]("ministryID")
        val allIDs = row.getAs[String]("allIDs").split(",").map(_.trim)
        if (allIDs.length > 1) {
          processMinistryFolder(ministryID, allIDs, baseDir)
        }
      }
    }

    def processMinistryFolder(ministryID: String, ids: Array[String], baseDir: String): Unit = {
      val ministryDir = new File(s"$baseDir/mdoid=$ministryID")
      if (!ministryDir.exists()) ministryDir.mkdirs()
      ids.drop(1).filter(id => id != null && id.trim.nonEmpty).foreach { id =>
        val orgFileName = getOrgName(id, broadcastOrgNamesDF)
        val sanitizedOrgFileName = orgFileName.replaceAll("[/\\\\]", "_")
        val sourceZipFilePath = s"$baseDir/mdoid=$id/$sanitizedOrgFileName.zip"
        val destinationZipFilePath = s"$ministryDir/$sanitizedOrgFileName.zip"
        val sourceFile = new File(sourceZipFilePath)
        if (sourceFile.exists()) {
          copyZipFile(sourceZipFilePath, destinationZipFilePath)
        }
        else {
          println(s"Source zip file $sourceZipFilePath does not exist.")
        }
      }
    }

    def copyZipFile(sourceZipPath: String, destinationZipPath: String): Unit = {
      try {
        Files.copy(Paths.get(sourceZipPath), Paths.get(destinationZipPath), StandardCopyOption.REPLACE_EXISTING)
      } catch {
        case e: Exception => println(s"Failed to copy $sourceZipPath: ${e.getMessage}")
      }
    }

    mergeMdoidFolders(orgHierarchyDF, destinationPath)

    // Zipping reports with password protection
    val password = conf.password
      if (mdoidFolders != null) {
    mdoidFolders.foreach { mdoidFolder =>
      if (mdoidFolder.isDirectory) {
        val zipFilePath = s"${mdoidFolder}"
        // Create a password-protected zip file for the mdoid folder
        val combinedZipFile = new ZipFile(zipFilePath + "/reports.zip", password.toCharArray)
        val parameters = new ZipParameters()
        parameters.setEncryptFiles(true)
        parameters.setEncryptionMethod(EncryptionMethod.ZIP_STANDARD)
        // Add all files within the mdoid folder to the zip file
        mdoidFolder.listFiles().foreach { file =>
          combinedZipFile.addFile(file, parameters)
        }
        val mdoid = mdoidFolder.getName
        println(s"Hierarchical password protected zip created for $mdoid: $zipFilePath")
        mdoidFolder.listFiles().foreach { file =>
          if (file.isFile && file.getName != "reports.zip") file.delete()
        }

        val zipReportPath = s"${conf.destinationDirectoryPath}/${mdoidFolder.getName}"
        syncReports(zipFilePath, zipReportPath)
        }
       }
      }
      else {
          println("No mdoid folders found in the given directory.")
        }

    // Deleting the temporary merged folder
    try {
      FileUtils.deleteDirectory(new File(destinationPath))
    } catch {
      case e: Exception => println(s"Error deleting directory: ${e.getMessage}")
    }
  }
}
