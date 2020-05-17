/**
 * Practice to read from local files and remote files in S3 into DataFrames in Apache Spark
 *
 * Input:
 *  - Local folder containing csv files of various countries
 *  - S3 bucket containing same csv files of various countries (e.g. mycountries)
 *
 * ---
 *
 * Output:
 * From union-ing 2 files:
 * Count: 10
 * +----------+-----------+---------+
 * |country_id|  countries|region_id|
 * +----------+-----------+---------+
 * |        CH|Switzerland|        1|
 * |        DE|    Germany|        1|
 * |        DK|    Denmark|        1|
 * |        FR|     France|        1|
 * |        CA|     Canada|        2|
 * |        CN|      China|        3|
 * |        HK|   HongKong|        3|
 * |        IN|      India|        3|
 * |        EG|      Egypt|        4|
 * |        IL|     Israel|        4|
 * +----------+-----------+---------+
 *
 * From path:
 * Count: 21
 * +----------+--------------------+---------+
 * |country_id|           countries|region_id|
 * +----------+--------------------+---------+
 * |        CH|         Switzerland|        1|
 * |        DE|             Germany|        1|
 * |        DK|             Denmark|        1|
 * |        FR|              France|        1|
 * |        IT|               Italy|        1|
 * |        NL|         Netherlands|        1|
 * |        UK|      United Kingdom|        1|
 * |        CA|              Canada|        2|
 * |        MX|              Mexico|        2|
 * |        US|United States of ...|        2|
 * |        CN|               China|        3|
 * |        HK|            HongKong|        3|
 * |        IN|               India|        3|
 * |        JP|               Japan|        3|
 * |        SG|           Singapore|        3|
 * |        EG|               Egypt|        4|
 * |        IL|              Israel|        4|
 * |        KW|              Kuwait|        4|
 * |        NG|             Nigeria|        4|
 * |        ZM|              Zambia|        4|
 * |        ZW|            Zimbabwe|        4|
 * +----------+--------------------+---------+
 *
 * From S3:
 * Count: 21
 * +----------+--------------------+---------+
 * |country_id|           countries|region_id|
 * +----------+--------------------+---------+
 * |        CH|         Switzerland|        1|
 * |        DE|             Germany|        1|
 * |        DK|             Denmark|        1|
 * |        FR|              France|        1|
 * |        IT|               Italy|        1|
 * |        NL|         Netherlands|        1|
 * |        UK|      United Kingdom|        1|
 * |        CA|              Canada|        2|
 * |        MX|              Mexico|        2|
 * |        US|United States of ...|        2|
 * |        CN|               China|        3|
 * |        HK|            HongKong|        3|
 * |        IN|               India|        3|
 * |        JP|               Japan|        3|
 * |        SG|           Singapore|        3|
 * |        EG|               Egypt|        4|
 * |        IL|              Israel|        4|
 * |        KW|              Kuwait|        4|
 * |        NG|             Nigeria|        4|
 * |        ZM|              Zambia|        4|
 * |        ZW|            Zimbabwe|        4|
 * +----------+--------------------+---------+
 *
 * Schema:
 * root
 * |-- country_id: string (nullable = true)
 * |-- countries: string (nullable = true)
 * |-- region_id: string (nullable = true)
 *
 * ---
 *
 * Learning Point #1: We use Hadoop in Apache Spark to read/write to s3
 *
 * Use % for Hadoop, as %% asks sbt to append the current scala version to the artifact
 *
 * libraryDependencies ++= Seq(
 * "org.apache.spark" %% "spark-core" % "2.4.5",
 * "org.apache.spark" %% "spark-sql" % "2.4.5",
 * "org.apache.hadoop" % "hadoop-common" % "2.7.7",
 * "org.apache.hadoop" % "hadoop-client" % "2.7.7",
 * "org.apache.hadoop" % "hadoop-aws" % "2.7.7"
 * )
 *
 * Note: hadoop-aws contains the class org.apache.hadoop.fs.s3a.S3AFileSystem which is used for s3 read/writes
 *
 * ---
 *
 * Learning Point #2:
 * Either use environment variables to configure Hadoop:
 *
 * Add the AWS keys to the~/.bash_profile file:
 * export AWS_ACCESS_KEY_ID=redacted
 * export AWS_SECRET_ACCESS_KEY=redacted
 *
 * Then use System.getenv() method to retrieve environment variable values.
 *
 * val accessKeyId = System.getenv("AWS_ACCESS_KEY_ID")
 * val secretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY")
 * sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", accessKeyId)
 * sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", secretAccessKey)
 *
 * Or sparkContent.hadoopConfiguration.set() to configure Hadoop like in the source code below:
 * // Concat files from S3 into a DataFrame
 *     spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "YOUR_ACCESS_KEY")
 *     spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "YOUR_SECRET_ACCESS_KEY")
 *
 * Note: s3a is faster and support larger files than s3n
 *
 * ---
 */


package com.delvinlow.sparkpractices

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructType}

object CountriesCounter {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("RatingsCounterDF")
      .config("spark.master", "local[*]")
      .getOrCreate()

    val schema = new StructType()
      .add("country_id", StringType, true)
      .add("countries", StringType, true)
      .add("region_id", StringType, true)

    // Concat DataFrames by union
    val countries = spark.read.format("csv").schema(schema).load("./data/countries/xab.csv")
    val countries2 = spark.read.format("csv").schema(schema).load("./data/countries/xac.csv")
    val results = countries.union(countries2)
    println("From union-ing 2 files:")
    println(f"Count: ${results.count}")
    results.sort("region_id", "country_id").show(30)

    // Create one DataFrame by reading all files from path
    val all_countries = spark.read.format("csv")
      .schema(schema)
      .load("./data/countries/x*.csv")
    println("From path:")
    println(f"Count: ${all_countries.count}")
    all_countries.sort("region_id", "country_id").show(30)

    // Concat files from S3 into a DataFrame
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "YOUR_ACCESS_KEY")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "YOUR_SECRET_ACCESS_KEY")
    val s3DF = spark.read.format("csv").schema(schema).load("s3a://mycountries/*.csv")
    println("From S3:")
    println(f"Count: ${s3DF.count}")
    s3DF.sort("region_id", "country_id").show(30)

    println("Schema:")
    s3DF.printSchema()
  }
}
