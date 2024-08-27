package org.ekstep.analytics.dashboard.nationallearningweek

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig}
import org.ekstep.analytics.framework.FrameworkContext


object NationalLearningWeekModel extends AbsDashboardModel {

  implicit val className: String = "org.ekstep.analytics.dashboard.nationallearningweek.NationalLearningWeekModel"

  override def name() = "NationalLearningWeekModel"

  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {

    // get previous month start and end dates
    val monthStart = conf.nationalLearningWeekStart
    val monthEnd = conf.nationalLearningWeekEnd

    //get karma points data and filter for specific month
    val karmaPointsDataDF = userKarmaPointsDataFrame()
      .filter(col("credit_date") >= monthStart && col("credit_date") <= monthEnd)
      .groupBy(col("userid")).agg(sum(col("points")).alias("total_points"), max(col("credit_date")).alias("last_credit_date"))

    show(karmaPointsDataDF, "this is the kp_data")


    //get user and user-org data
    var (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()


    // fetch user details like fullname, profileImg etc for users of selected orgs
    val userOrgData = userOrgDF
      .join(userDF, userOrgDF("userID") === userDF("userID"), "inner")
      .select(
        userOrgDF("userID").alias("userid"),
        userOrgDF("userOrgID").alias("org_id"),
        userOrgDF("fullName").alias("fullname"),
        userOrgDF("userOrgName").alias("org_name"),
        userOrgDF("professionalDetails.designation").alias("designation"),
        userOrgDF("userProfileImgUrl").alias("profile_image")
      )

    //join karma points details with user details and select required columns
    val userLeaderBoardDataDF = userOrgData.join(karmaPointsDataDF, Seq("userid"), "inner")
      .filter(col("org_id") =!= "")
      .select(userOrgData("userid"),
        userOrgData("org_id"),
        userOrgData("fullname"),
        userOrgData("designation"),
        userOrgData("org_name"),
        userOrgData("profile_image"),
        karmaPointsDataDF("total_points"),
        karmaPointsDataDF("last_credit_date"))


    val filteredUserLeaderBoardDataDF = userLeaderBoardDataDF
      .filter(col("total_points").isNotNull && col("total_points") > 0)
    show(filteredUserLeaderBoardDataDF, "finaluserdata")

    val windowSpecRank = Window.partitionBy("org_id").orderBy(desc("total_points"))

    // rank the users based on the points within each org
    val userLeaderBoardOrderedDataDF = filteredUserLeaderBoardDataDF.withColumn("rank", dense_rank().over(windowSpecRank))
    userLeaderBoardOrderedDataDF.show(false)

    // sort them based on their fullNames for each rank group within each org
    val windowSpecRow = Window.partitionBy("org_id").orderBy(col("rank"), col("last_credit_date").asc)
    val finalUserLeaderBoardDataDF = userLeaderBoardOrderedDataDF.withColumn("row_num", row_number.over(windowSpecRow))
    val selectedColUserLeaderboardDF = finalUserLeaderBoardDataDF.select(col("userid"), col("org_id"), col("fullname"), col("designation"),col("profile_image"), col("total_points"),
      col("last_credit_date"), col("rank"), col("row_num")).dropDuplicates("userid")
    show(selectedColUserLeaderboardDF, "orderedData")

    // write to cassandra National Learning Week user table
     writeToCassandra(selectedColUserLeaderboardDF, conf.cassandraUserKeyspace, conf.cassandraNLWUserLeaderboardTable)
    val mdoNLWLeaderBoardDF = filteredUserLeaderBoardDataDF
      .groupBy("org_id", "org_name")
      .agg(
        count("userid").as("total_users"),           // Count the number of users per org_id
        sum("total_points").as("total_points"),     // Sum of total_points for each org_id
        max("last_credit_date").as("last_credit_date") // Max last_credit_date for each org_id
      )

    show(mdoNLWLeaderBoardDF, "hellooo")

    //    val sizedDF = mdoNLWLeaderBoardDF.withColumn("size",
    //     when(col("total_users") > 7, "XL")
    //    .when(col("total_users").between(4, 6), "L")
    //    .when(col("total_users").between(3, 2), "M")
    //    .otherwise("S")
    //)

    val sizedDF = mdoNLWLeaderBoardDF.withColumn("size",
      when(col("total_users") > 7, "XL")
        .when(col("total_users") === 3, "L")
        .when(col("total_users") === 2, "M")
        .otherwise("S")
    )


    val windowSpec2 = Window.partitionBy("size").orderBy(
      col("total_points").desc,
      col("last_credit_date").asc)

    val rankedDF = sizedDF.withColumn("row_num", row_number().over(windowSpec2))
    val selectedColMdoLeaderboardDF = rankedDF.select(col("org_id"), col("size"), col("total_users"), col("org_name"), col("row_num"), col("total_points"), col("last_credit_date"))
    show(selectedColMdoLeaderboardDF, "select")
    writeToCassandra(selectedColMdoLeaderboardDF, conf.cassandraUserKeyspace, conf.cassandraNLWMdoLeaderboardTable)
  }
}


