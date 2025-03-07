package org.ekstep.analytics.dashboard.esformdata

import org.apache.spark.SparkContext
import org.apache.spark.sql.{ SparkSession, Column ,SaveMode}
import org.apache.spark.sql.functions._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig}
import org.ekstep.analytics.framework.FrameworkContext
import sys.process._
import com.fasterxml.jackson.databind.{ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import java.io.File
import scala.util.{Try, Success, Failure}
import scala.collection.JavaConverters._

  object EsFormDataModel extends AbsDashboardModel {

    implicit val className = "org.ekstep.analytics.dashboard.esformdatta.EsFormDataJob"

    override def name() = "EsFormDataModel"
    def processData(timestamp: Long) (implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {
      val mapper = new ObjectMapper()
      mapper.registerModule(DefaultScalaModule)

      var (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()
      val orgHierarchyData = orgHierarchyDataframe()

      val userData = userOrgDF
        .join(orgHierarchyData, Seq("userOrgID"), "left")
        .dropDuplicates("userID")

      val userDataDF = userData
        .select(
          col("userID"),
          col("userOrgID"),
          col("fullName").alias("Full_Name"),
          col("professionalDetails.designation").alias("Designation"),
          col("personalDetails.primaryEmail").alias("Email"),
          col("personalDetails.mobile").alias("Phone_Number"),
          col("ministry_name").alias("Ministry"),
          col("dept_name").alias("Department"),
          col("userOrgName").alias("Organization"),
          col("personalDetails.gender").alias("Gender")
        )
        .coalesce(1)

      val formIds = conf.esFormDataIds.split(",").map(_.trim).toList
      val scriptPath="/mount/data/analytics/scripts/"
      val filePath= "/mount/data/analytics/es-form-data/"
      val tasks = formIds.map { formId =>
       Try {
        val command = s"bash ${scriptPath}es-form-data.sh $formId"
        if (command.! == 0) {
          println(s"Successfully fetched data for form ID: $formId")
          val sourceFilePath = s"${filePath}es_$formId.json"

          // Read JSON safely
          val sourceJson = Try(scala.io.Source.fromFile(sourceFilePath).getLines().mkString) match {
            case Success(json) => json
            case Failure(ex) =>
              println(s"Error reading JSON file: ${ex.getMessage}")
              return
          }

          // Parse JSON safely
          val rootNode = Try(mapper.readTree(sourceJson)) match {
            case Success(node) => node
            case Failure(ex) =>
              println(s"Error parsing JSON: ${ex.getMessage}")
              return
          }

          val transformedJsonStrings = rootNode.elements().asScala.map { node =>
            val source = node.get("_source")
            val transformedNode = mapper.createObjectNode()
            transformedNode.set("formId", source.get("formId"))
            transformedNode.set("timestamp", source.get("timestamp"))
            transformedNode.set("dataObject", source.get("dataObject"))
            transformedNode.set("createdBy", source.get("createdBy"))
            mapper.writeValueAsString(transformedNode)
          }.toList

          import spark.implicits._
          val jsonDataset = spark.createDataset(transformedJsonStrings)

          // Read JSON with better error handling
          val df = Try(spark.read.option("multiline", "true").json(jsonDataset)) match {
            case Success(data) => data
            case Failure(ex) =>
              println(s"Error processing JSON dataset: ${ex.getMessage}")
              return
          }

          //df.printSchema()
          //df.show(false)

          val replaceNewlinesUDF = udf((text: String) => Option(text).map(_.replaceAll("\n", " ")).orNull)

          val fieldMappings = Map(
            "Name of your Cadre Controlling Authority" -> "Cadre Controlling Authority",
            "Address of the Cadre Controlling Authority" -> "Address of the Cadre Controlling Authority",
            "Address of Cadre Controlling Authority" -> "Address of the Cadre Controlling Authority",
            "Contact email id of the Nodal person in the cadre" -> "Contact email id of the Nodal person in the cadre",
            "Contact email id of Nodal person in the cadre" -> "Contact email id of the Nodal person in the cadre",
            "Contact number of the Nodal person in the cadre" -> "Contact number of the Nodal person in the cadre",
            "Contact number of Nodal person" -> "Contact number of the Nodal person in the cadre",
            "Service" -> "Service",
            "Select the year in which you have attended this program" -> "Year in which attended the program",
            "If from CSS, please select the level ( if not please select \"Not Applicable\")" -> "Level if CSS",
            "If from CSS, please select the level" -> "Level if CSS",
            "If from CSS,  please select the level" -> "Level if CSS",
            "Have you updated your iGOT profile page specially w.r.t. Date of Birth, Gender, Organisation, Official Address and Present Designation" -> "Updated IGOT Profile",
            "If Officers on the Central Staffing scheme, please provide details of Central Deputation Tenure (Starting date upto ) e.g. 1/1/ 2023 to 31/12/ 2027 . ( if not on Deputation please type \"NA\"" -> "If Central staffing scheme tenure of allotment",
            "If Officers on the Central Staffing scheme, please provide details of Central Deputation Tenure (Starting date upto )" -> "If Central staffing scheme tenure of allotment",
            "If Officers on the Central Staffing scheme, please provide details of Central Deputation Tenure  (Starting date upto )" -> "If Central staffing scheme tenure of allotment",
            "If Officers on the Central Staffing scheme, please provide details of Central Deputation Tenure (Starting date upto ) e.g. 1/1/ 2023 to 31/12/ 2027 . ( if not on Deputation please type \"NA\"b" -> "If Central staffing scheme tenure of allotment",
            "E-HRMS Id (if available, if not available please type \"NA\")" -> "E-HRMS Id",
            "E-HRMS Id (if available)" -> "E-HRMS Id",
            "If selected \"Any Other\" as service at S No. 5, please provide details of service here: (If not applicable, please tyepeNA)" -> "Any other Services",
            "Date of retirement" -> "Date of retirement",
            "If from CSSS, please select the level ( if not please select \"Not Applicable\")" -> "Level if CSSS",
            "If from CSSS, please select the level" -> "Level if CSSS",
            "Course ID and Name" -> "Name of the Program",
            "If from AIS, please mention the Batch in YYYY format, if not type \"NA\"" -> "Batch",
            "If from AIS, please mention the Batch in YYYY format" -> "Batch",
            "If you are opting for this program please number this program in the order of preference e.g 1/2/3....." -> "Preference number for the program",
            "If you are opting for this program please number this program in the order of preference" -> "Preference number for the program",
            "If you are opting for this program please number this program in the order of  preference" -> "Preference number for the program",
            "If from AIS, select the category" -> "AIS Category"
          )
          val dataObjectFields: Seq[Column] = df.schema.fields
            .find(_.name == "dataObject")
            .map(_.dataType)
            .collect {
              case struct: org.apache.spark.sql.types.StructType =>
                struct.fields.map { field =>
                  val originalName = field.name
                  val mappedName = fieldMappings.getOrElse(originalName, originalName)
                  if (!fieldMappings.contains(originalName)) {
                    println(s"Missing mapping for: $originalName (Using default: $mappedName)")
                  }
                  //    val sanitizedName = originalName.replaceAll("[^a-zA-Z0-9]", "_")
                  //   val mappedName = fieldMappings.getOrElse(originalName, sanitizedName)
                  val colName = s"`dataObject`.`$originalName`"

                  field.dataType match {
                    case _: org.apache.spark.sql.types.ArrayType =>
                      replaceNewlinesUDF(concat_ws(", ", lit("\""), col(colName), lit("\""))).alias(mappedName)
                    case _ =>
                      replaceNewlinesUDF(col(colName)).alias(mappedName)
                  }
                }
            }
            .getOrElse(Array.empty[Column])
            .toSeq

          val allColumns: Seq[Column] = Seq(
            col("formId"),
            col("createdBy"),
            col("timestamp")
          ) ++ dataObjectFields

          val flattenedDF = df.select(allColumns: _*)

          // Join with user data as before

          val joinedDF = userDataDF
            .join(flattenedDF, userDataDF("userID") === flattenedDF("createdBy"), "inner")
            .drop("userID", "userOrgID", "formId")

          val outputDir = s"${filePath}${formId}_data"

          Try {
            joinedDF.repartition(1)
              .write
              .mode(SaveMode.Overwrite)
              .format("csv")
              .option("quote", "\"")
              .option("escape", "\"")
              .option("header", true)
              .save(outputDir)
          } match {
            case Success(_) => println("CSV written successfully")
            case Failure(ex) =>
              println(s"Error writing CSV: ${ex.getMessage}")
              return
          }

          Try {
            val partFile = new File(outputDir).listFiles()
              .find(_.getName.startsWith("part-"))
              .map(_.getName)
              .getOrElse(throw new Exception("No part- file found"))

            val sourceFile = s"$outputDir/$partFile"
            val destinationFile = s"${filePath}${formId}_data.csv"

            s"mv $sourceFile $destinationFile".!
            s"rm -r $outputDir".!
            println(s"CSV moved to $destinationFile successfully")
          } match {
            case Success(_) => println("File operations completed")
            case Failure(ex) => println(s"Error in file operations: ${ex.getMessage}")
          }
        }
       } match {
         case Success(_) => // Continue processing
         case Failure(ex) => println(s"Error processing form ID: $formId - ${ex.getMessage}")
       }
      }
    }
  }