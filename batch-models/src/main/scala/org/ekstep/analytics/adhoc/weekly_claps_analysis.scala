
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, expr, when}
import org.apache.spark.storage.StorageLevel

import java.io.Serializable

class AvroFSCache(val path: String, val compression: String = "snappy") extends Serializable {
  def write(df: DataFrame, name: String): Unit = {
    df.write.mode(SaveMode.Overwrite).option("compression", compression).format("avro").save(s"${path}/${name}")
  }
  def load(name: String, spark: SparkSession): DataFrame = {
    spark.read.format("avro").load(s"${path}/${name}").persist(StorageLevel.MEMORY_ONLY)
    //spark.read.format("avro").load(s"${path}/${name}") // removed the persist to optimise memory usage
  }
}

object weekly_claps_analysis {

    // Create Spark Session
    val spark = SparkSession.builder()
      .appName("weekly_claps_analysis")
      .master("local[*]")
      .getOrCreate()

    // load user detail avro
    private val userDetailCache: AvroFSCache = new AvroFSCache("/Users/shishirsuman/Documents/weekly_claps_analysis", "uncompressed")
    val userDetail = userDetailCache.load("avro", spark)

    // Read CSV file into DataFrame
    var w1df = spark.read.format("csv").option("header", "true").load("/Users/shishirsuman/Documents/weekly_claps_analysis/w1.csv")
    var w2df = spark.read.format("csv").option("header", "true").load("/Users/shishirsuman/Documents/weekly_claps_analysis/w2.csv")
    var w3df = spark.read.format("csv").option("header", "true").load("/Users/shishirsuman/Documents/weekly_claps_analysis/w3.csv")
    var w4df = spark.read.format("csv").option("header", "true").load("/Users/shishirsuman/Documents/weekly_claps_analysis/w4.csv")
    var w5df = spark.read.format("csv").option("header", "true").load("/Users/shishirsuman/Documents/weekly_claps_analysis/w5.csv")
    var w6df = spark.read.format("csv").option("header", "true").load("/Users/shishirsuman/Documents/weekly_claps_analysis/w6.csv")
    var w7df = spark.read.format("csv").option("header", "true").load("/Users/shishirsuman/Documents/weekly_claps_analysis/w7.csv")
    var w8df = spark.read.format("csv").option("header", "true").load("/Users/shishirsuman/Documents/weekly_claps_analysis/w8.csv")
    var w9df = spark.read.format("csv").option("header", "true").load("/Users/shishirsuman/Documents/weekly_claps_analysis/w9.csv")

  // rename platformEngagementTime to wx_learning_hours
  w1df = w1df.withColumn("w1_claps",expr("CASE WHEN platformEngagementTime>=60 THEN 1 ELSE 0 END"))
    .select(
    col("userid").alias("user_id"),
    col("platformEngagementTime").alias("w1_learning_hours"),
      col("w1_claps")
  )

  println(w1df.show())

  w2df = w2df.withColumn("w2_claps",expr("CASE WHEN platformEngagementTime>=60 THEN 1 ELSE 0 END"))
    .select(
    col("userid").alias("user_id"),
    col("platformEngagementTime").alias("w2_learning_hours"),
      col("w2_claps")
  )

  println(w2df)

  w3df = w3df.withColumn("w3_claps", expr("CASE WHEN platformEngagementTime>=60 THEN 1 ELSE 0 END"))
    .select(
    col("userid").alias("user_id"),
    col("platformEngagementTime").alias("w3_learning_hours"),
      col("w3_claps")
  )

  println(w3df)

  w4df = w4df.withColumn("w4_claps", expr("CASE WHEN platformEngagementTime>=60 THEN 1 ELSE 0 END"))
    .select(
    col("userid").alias("user_id"),
    col("platformEngagementTime").alias("w4_learning_hours"),
      col("w4_claps")
  )

  println(w4df)

  w5df = w5df.withColumn("w5_claps", expr("CASE WHEN platformEngagementTime>=60 THEN 1 ELSE 0 END"))
    .select(
    col("userid").alias("user_id"),
    col("platformEngagementTime").alias("w5_learning_hours"),
      col("w5_claps")
  )

  println(w5df)

  w6df = w6df.withColumn("w6_claps", expr("CASE WHEN platformEngagementTime>=60 THEN 1 ELSE 0 END"))
    .select(
    col("userid").alias("user_id"),
    col("platformEngagementTime").alias("w6_learning_hours"),
      col("w6_claps")
  )

  println(w6df)

  w7df = w7df.withColumn("w7_claps", expr("CASE WHEN platformEngagementTime>=60 THEN 1 ELSE 0 END"))
    .select(
    col("userid").alias("user_id"),
    col("platformEngagementTime").alias("w7_learning_hours"),
      col("w7_claps")
  )

  println(w7df)

  w8df = w8df.withColumn("w8_claps", expr("CASE WHEN platformEngagementTime>=60 THEN 1 ELSE 0 END"))
    .select(
    col("userid").alias("user_id"),
    col("platformEngagementTime").alias("w8_learning_hours"),
      col("w8_claps")
  )

  println(w8df)

  w9df = w9df.withColumn("w9_claps", expr("CASE WHEN platformEngagementTime>=60 THEN 1 ELSE 0 END"))
    .select(
    col("userid").alias("user_id"),
    col("platformEngagementTime").alias("w9_learning_hours"),
    col("w9_claps")
  )

  println(w9df)

  // join all dfs
  var mergedDF = w1df.join(w2df, Seq("user_id"), "inner")
  mergedDF = mergedDF.join(w3df, Seq("user_id"), "inner")
  mergedDF = mergedDF.join(w4df, Seq("user_id"), "inner")
  mergedDF = mergedDF.join(w5df, Seq("user_id"), "inner")
  mergedDF = mergedDF.join(w6df, Seq("user_id"), "inner")
  mergedDF = mergedDF.join(w7df, Seq("user_id"), "inner")
  mergedDF = mergedDF.join(w8df, Seq("user_id"), "inner")
  mergedDF = mergedDF.join(w9df, Seq("user_id"), "inner")

  //mergedDF.withColumn("total_claps", expr("CASE WHEN w1_claps>=0 THEN total_claps = w1_claps WHEN w2_claps>0 THEN total_claps = total_claps + w2_claps ELSE total_claps = 0 END"))

  val resultDf = mergedDF.withColumn("total_claps",

    // Start from the first week's claps

    when(col("w1_claps") =!= 0, col("w1_claps"))

      .otherwise(0)

      .plus(

        // Check for week 2 claps and so on

        when(col("w2_claps") =!= 0, col("w2_claps")).otherwise(0)

      ).plus(

        when(col("w3_claps") =!= 0, col("w3_claps")).otherwise(0)

      ).plus(

        when(col("w4_claps") =!= 0, col("w4_claps")).otherwise(0)

      ).plus(

        when(col("w5_claps") =!= 0, col("w5_claps")).otherwise(0)

      ).plus(

        when(col("w6_claps") =!= 0, col("w6_claps")).otherwise(0)

      ).plus(

        when(col("w7_claps") =!= 0, col("w7_claps")).otherwise(0)

      ).plus(

        when(col("w8_claps") =!= 0, col("w8_claps")).otherwise(0)

      ).plus(

        when(col("w9_claps") =!= 0, col("w9_claps")).otherwise(0)

      )

  )

  println(resultDf.show())


  }

