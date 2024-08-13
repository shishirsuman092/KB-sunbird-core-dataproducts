package org.ekstep.analytics.dashboard.survey.npsUpgrade

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, current_date, expr, lit}
import org.apache.spark.sql.types.{DateType, StringType}
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig}
import org.ekstep.analytics.framework.FrameworkContext


object NpsUpgradeModel extends AbsDashboardModel {
  implicit val className: String = "org.ekstep.analytics.dashboard.survey.npsUpgrade.NpsUpgradeModel"

  override def name() = "NpsModel"
  def processData(timestamp: Long) (implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {

    val usersSubmittedRejectedNPSDF = npsUpgradedTriggerC1DataFrame() // gives user data from druid who have received the popup in last 15 days
    val usersEnrolledCompletedCourseDF = npsUpgradedTriggerC2DataFrame() // gives user data from cassandra who have enrolled / completed atleast 1 course in last 15 days
    val usersRatedCouseDF = npsUpgradedTriggerC3DataFrame() // gives user data who have rated atleast one course in last 15 days

    val usersSubmittedRejectedNPSDFCount = usersSubmittedRejectedNPSDF.count()
    println(s"DataFrame Count for set of users who have submitted the form in last 15 days: $usersSubmittedRejectedNPSDFCount")
    val usersEnrolledCompletedCourseDFCount = usersEnrolledCompletedCourseDF.count()
    println(s"DataFrame Count for set of users who have enrolled or completed into atleast 1 course in last 15 days: $usersEnrolledCompletedCourseDFCount")
    val usersRatedCouseDFCount = usersRatedCouseDF.count()
    println(s"DataFrame Count for set of users who have rated atleast 1 course in last 15 days: $usersRatedCouseDFCount")

    var df = usersEnrolledCompletedCourseDF.union(usersRatedCouseDF)
    df = df.dropDuplicates("userid")
    df = df.na.drop(Seq("userid"))

    var filteredDF = df.except(usersSubmittedRejectedNPSDF)
    filteredDF = filteredDF.na.drop(Seq("userid"))


    val totalCount = df.count()
    println(s"DataFrame Count for users who are eligible: $totalCount")

    val filteredCount = filteredDF.count()
    println(s"DataFrame Count for set of users who are eligible and not filled form: $filteredCount")

    // check if the feed for these users is already there
    val cassandraDF = userUpgradedFeedFromCassandraDataFrame()
    val existingFeedCount = cassandraDF.count()
    println(s"DataFrame Count for users who have feed data: $existingFeedCount")
    val storeToCassandraDF = filteredDF.except(cassandraDF)
    var filteredStoreToCassandraDF = storeToCassandraDF.filter(col("userid").isNotNull && col("userid") =!= "" && col("userid") =!= "''")
    filteredStoreToCassandraDF = filteredStoreToCassandraDF.dropDuplicates("userid")
    val finalFeedCount = filteredStoreToCassandraDF.count()
    println(s"DataFrame Count for final set of users to create feed: $finalFeedCount")

    //create an additional dataframe that has columns of user_feed table as we have to insert thes userIDS to user_feed table

    val additionalDF = filteredStoreToCassandraDF.withColumn("category", lit("NPS2"))
      .withColumn("id", expr("uuid()").cast(StringType))
      .withColumn("createdby", lit("platform_rating"))
      .withColumn("createdon", current_date())
      .withColumn("action", lit("{\"dataValue\":\"yes\",\"actionData\":{\"formId\":"+conf.platformRatingSurveyId+"}}"))
      .withColumn("expireon", lit(null).cast(DateType))
      .withColumn("priority", lit(1))
      .withColumn("status", lit("unread"))
      .withColumn("updatedby", lit(null).cast(StringType))
      .withColumn("updatedon", lit(null).cast(DateType))
      .withColumn("version", lit("v1"))
    show(additionalDF)

    // write the dataframe to cassandra user_feed table
    additionalDF.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> conf.cassandraUserFeedKeyspace , "table" -> conf.cassandraUserFeedTable))
      .mode("append")
      .save()

    // write the dataframe to cassandra user_feed_backup table
    additionalDF.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "sunbird_notifications" , "table" -> "notification_feed_history"))
      .mode("append")
      .save()
  }
}
