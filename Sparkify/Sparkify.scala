/**
 * Basic Data Exploration for a Music Streaming App called Sparkify using Apache Spark
 */

package com.delvinlow.sparkpractices.Sparkify

import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, IntegerType, LongType, TimestampType}

object Sparkify {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("Sparkify")
      .config("spark.master", "local[*]")
      .getOrCreate()

    val configuration = spark.sparkContext.getConf.getAll
    println(configuration.mkString("\n"))

    // Data Exploration
    val userLogs = spark.read.json("./src/res/data/sparkify_log_small.json").cache()
    userLogs.show(10)

    // Calculate Statistics by Hour
    val withTimestamps = userLogs
      .where("page == 'NextSong'")
      .withColumn("Date", to_timestamp((userLogs.col("ts") / lit(1000))
        .cast(TimestampType)))
    val withHoursOfDays = withTimestamps
      .withColumn("Hour of Day", hour(col("Date")))
    val countByHours = withHoursOfDays.
      groupBy("Hour of Day").
      count().
      sort("Hour of Day")
    countByHours.show(24)

    // See all distinct users id to identify whether there are any invalid users
    val distinctUsers = userLogs.select("userId").dropDuplicates().sort("userID")
    distinctUsers.show

    // Filter userIDs that are empty strings
    val validUsers = userLogs.where("userId <> ''")
    validUsers.select("userId").dropDuplicates().sort("userID").show()

    userLogs.printSchema()
    println(f"Total: ${userLogs.count}")

    // Use Window function to label records from a user into phase 1 (paid) and phase 0 (after downgrading to free), separated by an event (i.e. Submit Downgrade)
    // Before labelling (records of one user):
    val userKellyLogs = userLogs.select("userId", "firstName", "page", "level", "ts", "song").where("userId == 1138")
    userKellyLogs.show()

    val userLogsWithDowngrade = validUsers.withColumn("downgraded", when(expr("page == 'Submit Downgrade'"), 1).otherwise(0))
    userLogsWithDowngrade.show()
    val windowDowngrade = Window.partitionBy("userID").orderBy(desc("ts")).rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val userLogsByPhase = userLogsWithDowngrade.withColumn("Phase", sum("downgraded").over(windowDowngrade))
    userLogsByPhase.show()

    // After labelling (records of one user):
    userLogsByPhase.select("userId", "firstName", "page", "level", "song", "ts", "phase").where("userId == 1138").orderBy("ts").show(100)

    spark.close()
  }
}
