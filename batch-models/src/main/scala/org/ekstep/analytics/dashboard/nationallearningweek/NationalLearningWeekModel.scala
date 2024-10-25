package org.ekstep.analytics.dashboard.nationallearningweek

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig, Redis}
import org.ekstep.analytics.framework.FrameworkContext


object NationalLearningWeekModel extends AbsDashboardModel {

  implicit val className: String = "org.ekstep.analytics.dashboard.nationallearningweek.NationalLearningWeekModel"

  override def name() = "NationalLearningWeekModel"

  def processData(timestamp: Long)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {

    // get previous month start and end dates
    val monthStart = conf.nationalLearningWeekStart
    val monthEnd = conf.nationalLearningWeekEnd

    //get karma points data and filter for specific month
    val karmaPointsDataDF = cache.load("userKarmaPoints")
      .filter(col("credit_date") >= monthStart && col("credit_date") <= monthEnd)
      .groupBy(col("userid")).agg(sum(col("points")).alias("total_points"), max(col("credit_date")).alias("last_credit_date"))

    //get user and user-org data
    val (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()


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
    val userLeaderBoardDataDF = userOrgData.join(karmaPointsDataDF, Seq("userid"), "left")
      .filter(col("org_id") =!= "")
      .select(userOrgData("userid"),
        userOrgData("org_id"),
        userOrgData("fullname"),
        userOrgData("designation"),
        userOrgData("org_name"),
        userOrgData("profile_image"),
        karmaPointsDataDF("total_points"),
        karmaPointsDataDF("last_credit_date"))

    val windowSpecRank = Window.partitionBy("org_id").orderBy(desc("total_points"))

    // rank the users based on the points within each org
    val userLeaderBoardOrderedDataDF = userLeaderBoardDataDF.withColumn("rank", dense_rank().over(windowSpecRank))

    // sort them based on their fullNames for each rank group within each org
    val windowSpecRow = Window.partitionBy("org_id").orderBy(col("rank"), col("last_credit_date").asc)
    val finalUserLeaderBoardDataDF = userLeaderBoardOrderedDataDF.withColumn("row_num", row_number.over(windowSpecRow))
    val certificateCountByUserDF = Redis.getMapAsDataFrame("dashboard_content_certificates_issued_nlw_by_user", Schema.enrolmentCountByUserSchema)
    val eventCertificateCountByUserDF = Redis.getMapAsDataFrame("dashboard_event_certificates_issued_nlw_by_user", Schema.eventEnrolmentCountByUserSchema)
    val learningHoursByUserDF = Redis.getMapAsDataFrame("dashboard_content_learning_hours_nlw_by_user", Schema.learningHoursByUserSchema)
    val eventLearningHoursByUserDF = Redis.getMapAsDataFrame("dashboard_event_learning_hours_nlw_by_user", Schema.eventLearningHoursByUserSchema)
    val extendedColsUserDF = certificateCountByUserDF.join(learningHoursByUserDF, Seq("userID"), "inner").withColumnRenamed("userID", "userid")

    val extendedColsUserEventDF = eventCertificateCountByUserDF.join(eventLearningHoursByUserDF, Seq("user_id"), "inner").withColumnRenamed("user_id", "userid")

    val extendedColsUserContentEventDF = extendedColsUserEventDF.join(extendedColsUserDF, Seq("userid"), "outer")
      .withColumn("totalEventLearningHours1", coalesce(col("totalEventLearningHours").cast("double"), lit(0.00)))
      .withColumn("totalLearningHours1", coalesce(col("totalLearningHours").cast("double"), lit(0.00)))
      .withColumn("count1", coalesce(col("count").cast("int"), lit(0)))
      .withColumn("event_count1", coalesce(col("event_count").cast("int"), lit(0)))
      .withColumn("total_count", coalesce((col("count1")+col("event_count1")), lit(0)))
      .withColumn("total_learning_hours", coalesce((col("totalEventLearningHours1")+col("totalLearningHours1")), lit(0.00)))


    val userStatsDataDF = extendedColsUserContentEventDF.join(finalUserLeaderBoardDataDF, Seq("userid"), "right")
      .withColumnRenamed("total_learning_hours", "learning_hours")

    val selectedColUserLeaderboardDF = userStatsDataDF
      .select(
        col("userid"),
        col("org_id"),
        col("fullname"),
        col("designation"),
        col("profile_image"),
        coalesce(col("total_points"), lit(0)).alias("total_points"), // Replace null total_points with 0
        col("last_credit_date"),
        coalesce(col("rank"), lit(0)).alias("rank"), // Replace null rank with 0
        col("row_num"),
        coalesce(col("total_count"), lit(0)).alias("count"), // Replace null count with 0
        coalesce(round(col("learning_hours"), 2), lit(0)).alias("total_learning_hours") // Replace null learning_hours with 0
      )
      .dropDuplicates("userid")
    show(selectedColUserLeaderboardDF, "cols")
    // write to cassandra National Learning Week user table

    writeToCassandra(selectedColUserLeaderboardDF, conf.cassandraUserKeyspace, conf.cassandraNLWUserLeaderboardTable)
    val mdoNLWLeaderBoardDF = userLeaderBoardDataDF
      .groupBy("org_id", "org_name")
      .agg(
        count("userid").as("total_users"),           // Count the number of users per org_id
        sum("total_points").as("total_points"),     // Sum of total_points for each org_id
        max("last_credit_date").as("last_credit_date") // Max last_credit_date for each org_id
      )

    val sizedDF = mdoNLWLeaderBoardDF.withColumn("size",
      when(col("total_users") > 50000, "XL")
        .when(col("total_users").between(10000, 50000), "L")
        .when(col("total_users").between(1000, 10000), "M")
        .when(col("total_users").between(500, 1000), "S")
        .otherwise("XS")
    )

    val windowSpec2 = Window.partitionBy("size").orderBy(
      col("total_points").desc,
      col("last_credit_date").asc)

    val rankedDF = sizedDF.withColumn("row_num", row_number().over(windowSpec2))
    val selectedColMdoLeaderboardDF = rankedDF.select(col("org_id"), col("size"), col("total_users"), col("org_name"), col("row_num"), col("total_points"), col("last_credit_date"))
    writeToCassandra(selectedColMdoLeaderboardDF, conf.cassandraUserKeyspace, conf.cassandraNLWMdoLeaderboardTable)
  }
}