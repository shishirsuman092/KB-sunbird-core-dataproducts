package org.ekstep.analytics.dashboard.ministrymetrics

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig, Redis}
import org.ekstep.analytics.framework.FrameworkContext


object MinistryMetricsModel extends AbsDashboardModel {

  implicit val className: String = "org.ekstep.analytics.dashboard.MinistryMetricsModel"

  override def name() = "MinistryMetricsModel"

  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {

    import spark.implicits._
    //get user and user-org data
    var (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()
    var orgHierarchyCompleteDF = orgCompleteHierarchyDataFrame()
    var distinctMdoIDsDF = userOrgDF.select("userOrgID").distinct()
    println("The number of distinct MDO ids is: "+ distinctMdoIDsDF.count())

    val joinedDF = orgHierarchyCompleteDF.join(distinctMdoIDsDF, orgHierarchyCompleteDF("sborgid") === distinctMdoIDsDF("userOrgID"), "inner")
    println("The number of distinct orgs in orgHierarchy is: "+joinedDF.count())
    show(joinedDF, "orgHierarchyCompletedDF")


    val userSumDF = Redis.getMapAsDataFrame("dashboard_user_count_by_user_org", Schema.totalLearningHoursSchema)
    val userLoginpercentDF = Redis.getMapAsDataFrame("dashboard_login_percent_last_24_hrs_by_user_org", Schema.totalLearningHoursSchema)
    val certSumDF = Redis.getMapAsDataFrame("dashboard_certificates_generated_count_by_user_org", Schema.totalLearningHoursSchema)
    val enrolmentDF = Redis.getMapAsDataFrame("dashboard_enrolment_content_by_user_org", Schema.totalLearningHoursSchema)


    def processOrgsL3(df: DataFrame, userOrgDF: DataFrame, orgHierarchyCompleteDF: DataFrame): DataFrame = {

      val organisationDF = df.dropDuplicates()
      val sumDF = organisationDF

      val userJoinedDF = sumDF.join(userSumDF, col("userOrgID") === col("organisationID"), "left_outer")
        .groupBy("organisationID")
        .agg(sum(col("totalLearningHours")).alias("learningSumValue"))

      val loginJoinedDF = userJoinedDF
        .join(userLoginpercentDF, col("userOrgID") === col("organisationID"), "left_outer")
        .groupBy("organisationID", "learningSumValue")
        .agg(sum(col("totalLearningHours")).alias("loginSumValue"))


      // Join loginJoinedDF with certSumDF for certSumValue
      val certJoinedDF = loginJoinedDF
        .join(certSumDF, col("userOrgID") === col("organisationID"), "left_outer")
        .groupBy("organisationID", "learningSumValue", "loginSumValue")
        .agg(sum(col("totalLearningHours")).alias("certSumValue"))

      // Join certJoinedDF with enrolmentSumDF for enrolmentSumValue
      val finalResultDF = certJoinedDF
        .join(enrolmentDF, col("userOrgID") === col("organisationID"), "left_outer")
        .groupBy("organisationID","learningSumValue", "loginSumValue", "certSumValue")
        .agg(sum(col("totalLearningHours")).alias("enrolmentSumValue"))
        .withColumn("allIDs", lit(null).cast("string"))
        .select(col("organisationID").alias("ministryID"), col("allIDs"), col("learningSumValue"), col("loginSumValue"), col("certSumValue"), col("enrolmentSumValue"))

      show(finalResultDF, "finalresult")

      val finaldf2 = finalResultDF
        .withColumn("learningSumValue", col("learningSumValue").cast("int"))
        .withColumn("loginSumValue", coalesce(col("loginSumValue").cast("int"), lit(0)))
        .withColumn("certSumValue", coalesce(col("certSumValue").cast("int"), lit(0)))
        .withColumn("enrolmentSumValue", coalesce(col("enrolmentSumValue").cast("int"),lit(0)))
      finaldf2



    }

    def processDepartmentL2(df: DataFrame, userOrgDF: DataFrame, orgHierarchyCompleteDF: DataFrame): DataFrame = {
      val organisationDF = df
        .join(orgHierarchyCompleteDF, df("departmentMapID") === orgHierarchyCompleteDF("l2mapid"), "left")
        .select(df("departmentID"), col("sborgid").alias("organisationID")).dropDuplicates()

      val sumDF = organisationDF
        .groupBy("departmentID")
        .agg(
          concat_ws(",", collect_set(when(col("organisationID").isNotNull, col("organisationID")))).alias("orgIDs")
        )
        .withColumn("associatedIds", concat_ws(",", col("orgIDs")))
        .withColumn("allIDs", concat_ws(",", col("departmentID"), col("associatedIds")))

      val userJoinedDF = sumDF.withColumn("orgID", explode(split(col("allIDs"), ",")))
        .join(userSumDF, col("userOrgID") === col("orgID"), "left_outer")
        .groupBy("departmentID", "allIDs")
        .agg(sum(col("totalLearningHours")).alias("learningSumValue"))

      val loginJoinedDF = userJoinedDF
        .withColumn("orgID", explode(split(col("allIDs"), ",")))
        .join(userLoginpercentDF, col("userOrgID") === col("orgID"), "left_outer")
        .groupBy("departmentID", "allIDs", "learningSumValue")
        .agg(sum(col("totalLearningHours")).alias("loginSumValue"))

      // Join loginJoinedDF with certSumDF for certSumValue
      val certJoinedDF = loginJoinedDF
        .withColumn("orgID", explode(split(col("allIDs"), ",")))
        .join(certSumDF, col("userOrgID") === col("orgID"), "left_outer")
        .groupBy("departmentID", "allIDs", "learningSumValue", "loginSumValue")
        .agg(sum(col("totalLearningHours")).alias("certSumValue"))

      // Join certJoinedDF with enrolmentSumDF for enrolmentSumValue
      val finalResultDF = certJoinedDF
        .withColumn("orgID", explode(split(col("allIDs"), ",")))
        .join(enrolmentDF, col("userOrgID") === col("orgID"), "left_outer")
        .groupBy("departmentID", "allIDs", "learningSumValue", "loginSumValue", "certSumValue")
        .agg(sum(col("totalLearningHours")).alias("enrolmentSumValue"))
        .select(col("departmentID").alias("ministryID"), col("allIDs"), col("learningSumValue"), col("loginSumValue"), col("certSumValue"), col("enrolmentSumValue"))
      show(finalResultDF, "finalresult")

      val finaldf2 = finalResultDF
        .withColumn("learningSumValue", col("learningSumValue").cast("int"))
        .withColumn("loginSumValue", coalesce(col("loginSumValue").cast("int"), lit(0)))
        .withColumn("certSumValue", coalesce(col("certSumValue").cast("int"), lit(0)))
        .withColumn("enrolmentSumValue", coalesce(col("enrolmentSumValue").cast("int"),lit(0)))

      finaldf2

    }

    def processMinistryL1(df: DataFrame, userOrgDF: DataFrame, orgHierarchyCompleteDF: DataFrame): DataFrame = {

      println("Processing Ministry L1 DataFrame:")
      val departmentAndMapIDsDF = df
        .join(orgHierarchyCompleteDF, df("ministryMapID") === orgHierarchyCompleteDF("l1mapid"), "left")
        .select(df("ministryID"), col("sborgid").alias("departmentID"), col("mapid").alias("departmentMapID"))

      // Join with orgHierarchyCompleteDF to get the organisationDF
      val organisationDF = departmentAndMapIDsDF
        .join(orgHierarchyCompleteDF, departmentAndMapIDsDF("departmentMapID") === orgHierarchyCompleteDF("l2mapid"), "left")
        .select(departmentAndMapIDsDF("ministryID"), departmentAndMapIDsDF("departmentID"),col("sborgid").alias("organisationID")).dropDuplicates()
      show(organisationDF, "hierarchyF")


      val sumDF = organisationDF
        .groupBy("ministryID")
        .agg(
          concat_ws(",", collect_set(when(col("departmentID").isNotNull, col("departmentID")))).alias("departmentIDs"),
          concat_ws(",", collect_set(when(col("organisationID").isNotNull, col("organisationID")))).alias("orgIDs")
        )
        .withColumn("associatedIds", concat_ws(",", col("departmentIDs"), col("orgIDs")))
        .withColumn("allIDs", concat_ws(",", col("ministryID"), col("associatedIds")))


      val userJoinedDF = sumDF.withColumn("orgID", explode(split(col("allIDs"), ",")))
        .join(userSumDF, col("userOrgID") === col("orgID"), "left_outer")
        .groupBy("ministryID", "allIDs")
        .agg(sum(col("totalLearningHours")).alias("learningSumValue"))

      val loginJoinedDF = userJoinedDF
        .withColumn("orgID", explode(split(col("allIDs"), ",")))
        .join(userLoginpercentDF, col("userOrgID") === col("orgID"), "left_outer")
        .groupBy("ministryID", "allIDs", "learningSumValue")
        .agg(sum(col("totalLearningHours")).alias("loginSumValue"))

      // Join loginJoinedDF with certSumDF for certSumValue
      val certJoinedDF = loginJoinedDF
        .withColumn("orgID", explode(split(col("allIDs"), ",")))
        .join(certSumDF, col("userOrgID") === col("orgID"), "left_outer")
        .groupBy("ministryID", "allIDs", "learningSumValue", "loginSumValue")
        .agg(sum(col("totalLearningHours")).alias("certSumValue"))

      // Join certJoinedDF with enrolmentSumDF for enrolmentSumValue
      val finalResultDF = certJoinedDF
        .withColumn("orgID", explode(split(col("allIDs"), ",")))
        .join(enrolmentDF, col("userOrgID") === col("orgID"), "left_outer")
        .groupBy("ministryID", "allIDs", "learningSumValue", "loginSumValue", "certSumValue")
        .agg(sum(col("totalLearningHours")).alias("enrolmentSumValue"))
        .select(col("ministryID"), col("allIDs"), col("learningSumValue"), col("loginSumValue"), col("certSumValue"), col("enrolmentSumValue"))
      show(finalResultDF, "finalresult")

      val finaldf2 = finalResultDF
        .withColumn("learningSumValue", col("learningSumValue").cast("int"))
        .withColumn("loginSumValue", coalesce(col("loginSumValue").cast("int"), lit(0)))
        .withColumn("certSumValue", coalesce(col("certSumValue").cast("int"), lit(0)))
        .withColumn("enrolmentSumValue", coalesce(col("enrolmentSumValue").cast("int"),lit(0)))

      finaldf2
    }


    //Create DataFrames based on conditions
    val ministryL1DF = joinedDF.filter(col("sborgtype") === "ministry").select(col("sborgid").alias("ministryID"), col("mapid").alias("ministryMapID"))
    show(ministryL1DF, "MinsitryData")
    val ministryOrgDF = processMinistryL1(ministryL1DF, userOrgDF, orgHierarchyCompleteDF)
    val departmentL2DF = joinedDF.filter((col("sborgtype") === "department") || (col("sborgsubtype") === "department")).select(col("sborgid").alias("departmentID"), col("mapid").alias("departmentMapID"))
    show(departmentL2DF, "DepartmentData")
    val deptOrgDF =  processDepartmentL2(departmentL2DF, userOrgDF, orgHierarchyCompleteDF)
    val orgsL3DF = joinedDF.filter((col("sborgtype") === "mdo")  && (col("sborgsubtype") !== "department")).select(col("sborgid").alias("organisationID"))
    show(orgsL3DF, "OrgData")
    val orgsDF = processOrgsL3(orgsL3DF, userOrgDF, orgHierarchyCompleteDF)

    var combinedMinistryMetricsDF = ministryOrgDF.union(deptOrgDF).union(orgsDF)

    show(combinedMinistryMetricsDF, "MinistryMetrics")

    Redis.dispatchDataFrame[Int]("dashboard_rolled_up_user_count", combinedMinistryMetricsDF, "ministryID", "learningSumValue")

    Redis.dispatchDataFrame[Int]("dashboard_rolled_up_login_percent_last_24_hrs", combinedMinistryMetricsDF, "ministryID", "loginSumValue")

    Redis.dispatchDataFrame[Double]("dashboard_rolled_up_certificates_generated_count", combinedMinistryMetricsDF, "ministryID", "certSumValue")

    Redis.dispatchDataFrame[Double]("dashboard_rolled_up_enrolment_content_count", combinedMinistryMetricsDF, "ministryID", "enrolmentSumValue")

  }
}