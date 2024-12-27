package org.ekstep.analytics.dashboard.survey.nps

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, current_date, expr, lit}
import org.apache.spark.sql.types.{DateType, StringType}
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{AbsDashboardModel, DashboardConfig}
import org.ekstep.analytics.framework.FrameworkContext


object NpsModel extends AbsDashboardModel {
  implicit val className: String = "org.ekstep.analytics.dashboard.survey.nps.NpsModel"

  override def name() = "NpsModel"
  def processData(timestamp: Long) (implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, conf: DashboardConfig): Unit = {

    val druidData1 = npsTriggerC1DataFrame() //gives data from druid for users who have submitted the survey form in last 3 months
    val druidData2 = npsTriggerC2DataFrame() // gives data from druid for users who have either completed 1 course or have more than 30 telemetry events
    val mongodbData = npsTriggerC3DataFrame() // gives the data from mongoDB for the users who have posted atleast 1 discussion

    var df = druidData2.union(mongodbData)
    df = df.dropDuplicates("userid")
    val druidData1Count = druidData1.count()
    println(s"DataFrame Count for set of users who have submitted the form: $druidData1Count")
    val druidData2Count = druidData2.count()
    println(s"DataFrame Count for set of users who have completed a course or have had atleast 15 sessions: $druidData2Count")
    val mongodbDataCount = mongodbData.count()
    println(s"DataFrame Count for set of users who have posted atleast 1 discussion: $mongodbDataCount")
    val totalCount = df.count()
    println(s"DataFrame Count for users who are eligible: $totalCount")
    val filteredDF = df.except(druidData1)
    val filteredCount = filteredDF.count()
    println(s"DataFrame Count for set of users who are eligible and not filled form: $filteredCount")
   // check if the feed for these users is already there
    val cassandraDF = userFeedFromCassandraDataFrame()

    val existingFeedCount = cassandraDF.count()
    println(s"DataFrame Count for users who have feed data: $existingFeedCount")
    val storeToCassandraDF = filteredDF.except(cassandraDF)
    var filteredStoreToCassandraDF = storeToCassandraDF.filter(col("userid").isNotNull && col("userid") =!= "" && col("userid") =!= "''")
    filteredStoreToCassandraDF = filteredStoreToCassandraDF.dropDuplicates("userid")
    val finalFeedCount = filteredStoreToCassandraDF.count()
    println(s"DataFrame Count for final set of users to create feed: $finalFeedCount")
    //create an additional dataframe that has columns of user_feed table as we have to insert thes userIDS to user_feed table
 
    val additionalDF = filteredStoreToCassandraDF.withColumn("category", lit("NPS"))
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
