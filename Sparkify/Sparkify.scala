/** Basic Data Exploration for a Music Streaming App called Sparkify using Apache Spark
 *
 * Input:
 * Logs of user activity with 10k records
 *
 * Output (Example):
 *
 * Songs Played by Hour of Day
 * +-----------+-----+
 * |Hour of Day|count|
 * +-----------+-----+
 * |          0|  484|
 * |          1|  430|
 * |          2|  362|
 * |          3|  295|
 * |          4|  257|
 * |          5|  248|
 * |          6|  369|
 * |          7|  375|
 * |          8|  456|
 * |          9|  454|
 * |         10|  382|
 * |         11|  302|
 * |         12|  352|
 * |         13|  276|
 * |         14|  348|
 * |         15|  358|
 * |         16|  375|
 * |         17|  249|
 * |         18|  216|
 * |         19|  228|
 * |         20|  251|
 * |         21|  339|
 * |         22|  462|
 * |         23|  479|
 * +-----------+-----+
 *
 * Schema:
 * root
 * |-- artist: string (nullable = true)
 * |-- auth: string (nullable = true)
 * |-- firstName: string (nullable = true)
 * |-- gender: string (nullable = true)
 * |-- itemInSession: long (nullable = true)
 * |-- lastName: string (nullable = true)
 * |-- length: double (nullable = true)
 * |-- level: string (nullable = true)
 * |-- location: string (nullable = true)
 * |-- method: string (nullable = true)
 * |-- page: string (nullable = true)
 * |-- registration: long (nullable = true)
 * |-- sessionId: long (nullable = true)
 * |-- song: string (nullable = true)
 * |-- status: long (nullable = true)
 * |-- ts: long (nullable = true)
 * |-- userAgent: string (nullable = true)
 * |-- userId: string (nullable = true)
 *
 * Total: 10000
 */

package com.delvinlow.sparkpractices.Sparkify

import org.apache.spark.sql.SparkSession
import org.apache.log4j._
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
//    userLogs.show(10)

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

    userLogs.printSchema()
    println(f"Total: ${userLogs.count}")

    spark.close()
  }
}
