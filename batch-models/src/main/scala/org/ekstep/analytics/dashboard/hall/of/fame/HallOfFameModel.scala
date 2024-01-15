package org.ekstep.analytics.dashboard.hall.of.fame

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._
import org.ekstep.analytics.dashboard.DashboardUtil._
import org.ekstep.analytics.dashboard.DataUtil._
import org.ekstep.analytics.dashboard.{DashboardConfig, DummyInput, DummyOutput}
import org.ekstep.analytics.framework.{FrameworkContext, IBatchModelTemplate}

object HallOfFameModel extends IBatchModelTemplate[String, DummyInput, DummyOutput, DummyOutput] with Serializable {

  implicit val className: String = "org.ekstep.analytics.dashboard.hall.of.fame.HallOfFameModel"

  override def name() = "HallOfFameModel"

  override def preProcess(events: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyInput] = {
    val executionTime = System.currentTimeMillis()
    sc.parallelize(Seq(DummyInput(executionTime)))
  }

  override def algorithm(events: RDD[DummyInput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    val timestamp = events.first().timestamp // extract timestamp from input
    implicit val spark: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()
    processHallOfFame(timestamp, config)
    sc.parallelize(Seq()) // return empty rdd
  }

  override def postProcess(events: RDD[DummyOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    sc.parallelize(Seq())
  }

  def processHallOfFame(timestamp: Long, config: Map[String, AnyRef]) (implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    println(config)
    implicit val conf: DashboardConfig = parseConfig(config)
    if (conf.debug == "true") debug = true // set debug to true if explicitly specified in the config
    if (conf.validation == "true") validation = true // set validation to true if explicitly specified in the config

    // get month start and end dates
    val monthStart = date_format(date_trunc("MONTH", add_months(current_date(), -1)), "yyyy-MM-dd HH:mm:ss")
    val monthEnd = date_format(last_day(add_months(current_date(), -1)), "yyyy-MM-dd 23:59:59")

    // get karma points data
    val karmaPointsData = userKarmaPointsDataFrame().filter(col("credit_date") >= monthStart && col("credit_date") <= monthEnd)

    // get user-org data
    var (orgDF, userDF, userOrgDF) = getOrgUserDataFrames()
    val userOrgData = userOrgDF.select(col("userID"), col("userOrgID").alias("org_id"), col("userOrgName").alias("org_name"))

    var df = karmaPointsData.join(userOrgData, karmaPointsData.col("userid").equalTo(userOrgData.col("userID")), "full")
      .select(col("userID"),col("points"), col("org_id"), col("org_name"), col("credit_date"))
      .filter(col("org_id") =!= "")

    // calculate average karma points - MDO wise
    df = df.groupBy(col("org_id"), col("org_name"))
      .agg(sum(col("points")).alias("total_kp"), countDistinct(col("userID")).alias("total_users"), max(col("credit_date")).alias("latest_credit_date"))
    df = df.withColumn("average_kp", col("total_kp") / col("total_users"))

    // store Hall of Fame data in cassandra
    df = df.withColumn("month", month(monthStart))
      .withColumn("year", year(monthStart))
      .withColumn("kp_last_calculated", current_timestamp())

    writeToCassandra(df, conf.cassandraUserKeyspace, conf.cassandraHallOfFameTable)

  }
}