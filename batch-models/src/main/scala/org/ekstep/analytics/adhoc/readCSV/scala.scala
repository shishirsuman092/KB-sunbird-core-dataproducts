package org.ekstep.analytics.adhoc.readCSV

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.col

object scala {


  // Create Spark Session
  val spark = SparkSession.builder()
    .appName("readCSV")
    .master("local[*]")
    .getOrCreate()

  // Read CSV file into DataFrame
  val df = spark.read
    .format("csv").load("/Users/shishirsuman/Downloads/2024-10-27-NLW-user-leaderboard/part-00000-9b3186b9-c7aa-403a-862f-9cb5b0856949-c000.csv")


  // Show the DataFrame
  df.show()

  val kbDF = df.filter(col("org_id") === "0133783095823810560")

  println(kbDF, "kbDF")

  val anshuKBdf = spark.read
    .format("csv").load("/Users/shishirsuman/Downloads/kb-26102024.csv")

  val anshuDF = anshuKBdf
    .select(
      col("user_id").alias("userid"),
      col("total_learning_hours").alias("anshu_total_learning_hours"),
      col("completions").alias("completions")
    )

  val mergedDF = kbDF.join(anshuDF, Seq("userid"), "inner")

  println(mergedDF, "mergedDF")

  mergedDF.coalesce(1).write.mode(SaveMode.Overwrite).format("csv").option("header", true).save(s"/tmp/2-NLW-user-leaderboard")

}
