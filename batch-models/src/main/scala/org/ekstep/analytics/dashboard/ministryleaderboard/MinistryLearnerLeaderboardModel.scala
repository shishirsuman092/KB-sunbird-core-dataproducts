package org.ekstep.analytics.dashboard.ministryleaderboard

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig, Redis}
import org.ekstep.analytics.framework.FrameworkContext


object MinistryLearnerLeaderboardModel extends AbsDashboardModel {

  implicit val className: String = "org.ekstep.analytics.dashboard.leaderboard.ministryleaderboard.MinistryLearnerLeaderboardModel"

  override def name() = "MinistryLearnerLeaderboardModel"

  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {


    //get user and user-org data
    var (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()
    var orgHierarchyCompleteDF = orgCompleteHierarchyDataFrame()
    var distinctMdoIDsDF = userOrgDF.select("userOrgID").distinct()
    println("The number of distinct MDO ids is: "+ distinctMdoIDsDF.count())

    val joinedDF = orgHierarchyCompleteDF.join(distinctMdoIDsDF, orgHierarchyCompleteDF("sborgid") === distinctMdoIDsDF("userOrgID"), "inner")
    println("The number of distinct orgs in orgHierarchy is: "+joinedDF.count())
    show(joinedDF, "orgHierarchyCompletedDF")

    // get previous month start and end dates
    val monthStart = date_format(date_trunc("MONTH", add_months(current_date(), -1)), "yyyy-MM-dd HH:mm:ss")
    val monthEnd = date_format(last_day(add_months(current_date(), -1)), "yyyy-MM-dd 23:59:59")

    //get previous month and year values
    val (month, year) = (
      date_format(date_add(last_day(add_months(current_date(), -1)), 1), "M"),
      date_format(add_months(current_date(), -1), "yyyy")
    )

    //get karma points data and filter for specific month
    val karmaPointsDataDF = userKarmaPointsDataFrame()
      .filter(col("credit_date") >= monthStart && col("credit_date") <= monthEnd)
      .groupBy(col("userid")).agg(sum(col("points")).alias("total_points"), max(col("credit_date")).alias("last_credit_date"))
    show(karmaPointsDataDF, "this is the kp_data")

    def processOrgsL3(df: DataFrame, userOrgDF: DataFrame, orgHierarchyCompleteDF: DataFrame): DataFrame = {
      val organisationDF = df.dropDuplicates()
      val userOrgData = userOrgDF.join(organisationDF, userOrgDF("userOrgID") === organisationDF("organisationID"), "inner")
        .select(col("userID"),col("organisationID").alias("userParentID"), col("professionalDetails.designation").alias("designation"),
          col("userProfileImgUrl"), col("fullName"), col("userOrgName")).distinct()

      userOrgData
    }

    def processDepartmentL2(df: DataFrame, userOrgDF: DataFrame, orgHierarchyCompleteDF: DataFrame): DataFrame = {
      val organisationDF = df
        .join(orgHierarchyCompleteDF, df("departmentMapID") === orgHierarchyCompleteDF("l2mapid"), "inner")
        .select(df("departmentID"), col("sborgid").alias("organisationID")).dropDuplicates()

      val userOrgData = userOrgDF.join(organisationDF, (userOrgDF("userOrgID") === organisationDF("departmentID")) ||
          (userOrgDF("userOrgID") === organisationDF("organisationID")), "inner")
        .select(col("userID"),col("departmentID").alias("userParentID"), col("professionalDetails.designation").alias("designation"),
          col("userProfileImgUrl"), col("fullName"), col("userOrgName")).distinct()

      userOrgData
    }

    def processMinistryL1(df: DataFrame, userOrgDF: DataFrame, orgHierarchyCompletedDF: DataFrame): DataFrame = {
      println("Processing Ministry L1 DataFrame:")
      val departmentAndMapIDsDF = df
        .join(orgHierarchyCompleteDF, df("ministryMapID") === orgHierarchyCompleteDF("l1mapid"), "left")
        .select(df("ministryID"), col("sborgid").alias("departmentID"), col("mapid").alias("departmentMapID"))

      // Join with orgHierarchyCompleteDF to get the organisationDF
      val organisationDF = departmentAndMapIDsDF
        .join(orgHierarchyCompleteDF, departmentAndMapIDsDF("departmentMapID") === orgHierarchyCompleteDF("l2mapid"), "left")
        .select(departmentAndMapIDsDF("ministryID"), departmentAndMapIDsDF("departmentID"),col("sborgid").alias("organisationID")).dropDuplicates()

      val userOrgData = userOrgDF.join(organisationDF, (userOrgDF("userOrgID") === organisationDF("ministryID")) ||
          (userOrgDF("userOrgID") === organisationDF("departmentID")) || (userOrgDF("userOrgID") === organisationDF("organisationID")), "inner")
        .select(col("userID"),col("ministryID").alias("userParentID"), col("professionalDetails.designation").alias("designation"),
          col("userProfileImgUrl"), col("fullName"), col("userOrgName")).distinct()

      userOrgData
    }




    //Create DataFrames based on conditions
    val ministryL1DF = joinedDF.filter(col("sborgtype") === "ministry").select(col("sborgid").alias("ministryID"), col("mapid").alias("ministryMapID"))
    show(ministryL1DF, "MinsitryData")
    val ministryOrgDF = processMinistryL1(ministryL1DF, userOrgDF, orgHierarchyCompleteDF)
    val departmentL2DF = joinedDF.filter(col("sborgtype") === "department").select(col("sborgid").alias("departmentID"), col("mapid").alias("departmentMapID"))
    show(departmentL2DF, "DepartmentData")
    val deptOrgDF =  processDepartmentL2(departmentL2DF, userOrgDF, orgHierarchyCompleteDF)
    val orgsL3DF = joinedDF.filter((col("sborgtype") === "mdo") && (col("sborgsubtype") !== "department")).select(col("sborgid").alias("organisationID"))
    show(orgsL3DF, "OrgData")
    val orgsDF = processOrgsL3(orgsL3DF, userOrgDF, orgHierarchyCompleteDF)

    val userOrgData = ministryOrgDF.union(deptOrgDF).union(orgsDF)

    var userLeaderBoardDataDF = userOrgData.join(karmaPointsDataDF, userOrgData("userID") === karmaPointsDataDF("userid"), "inner")
      .filter(col("userParentID") =!= "")
      .select(userOrgData("userID").alias("userid"),
        userOrgData("userParentID").alias("org_id"),
        userOrgData("fullName").alias("fullname"),
        userOrgData("userProfileImgUrl").alias("profile_image"),
        userOrgData("userOrgName").alias("org_name"),
        userOrgData("designation"),
        karmaPointsDataDF("total_points"),
        karmaPointsDataDF("last_credit_date"))
      .withColumn("month", (month - 1).cast("int"))
      .withColumn("year", lit(year))

    val windowSpecRank = Window.partitionBy("org_id").orderBy(desc("total_points"))
    userLeaderBoardDataDF = userLeaderBoardDataDF.withColumn("rank", dense_rank().over(windowSpecRank))
    userLeaderBoardDataDF.show(false)

    // sort them based on their fullNames for each rank group within each org
    val windowSpecRow = Window.partitionBy("org_id").orderBy(col("rank"), col("last_credit_date").desc)
    userLeaderBoardDataDF = userLeaderBoardDataDF.withColumn("row_num", row_number.over(windowSpecRow))


    // write to cassandra ministry leaner leaderboard
    writeToCassandra(userLeaderBoardDataDF, conf.cassandraUserKeyspace, conf.cassandraMDOLearnerLeaderboardTable)
  }
}