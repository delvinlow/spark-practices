/**
 * Count up how many of each star rating exists in the MovieLens 100K data set, which contains 100k records of movies ratings.
 *
 * Input:
 * • MovieLens 100K data set - 4 columns delimited by whitespaces without header. (Not included in repo)
 *
 * Output:
 * Total Count: 100000
 * +--------+-------+------+---------+
 * |moviesID|usersID|rating|timestamp|
 * +--------+-------+------+---------+
 * |     196|    242|     3|881250949|
 * |     186|    302|     3|891717742|
 * |      22|    377|     1|878887116|
 * |     244|     51|     2|880606923|
 * |     166|    346|     1|886397596|
 * |     298|    474|     4|884182806|
 * |     115|    265|     2|881171488|
 * |     253|    465|     5|891628467|
 * |     305|    451|     3|886324817|
 * |       6|     86|     3|883603013|
 * |      62|    257|     2|879372434|
 * |     286|   1014|     5|879781125|
 * |     200|    222|     5|876042340|
 * |     210|     40|     3|891035994|
 * |     224|     29|     3|888104457|
 * |     303|    785|     3|879485318|
 * |     122|    387|     5|879270459|
 * |     194|    274|     2|879539794|
 * |     291|   1042|     4|874834944|
 * |     234|   1184|     2|892079237|
 * +--------+-------+------+---------+
 * only showing top 20 rows
 *
 * Count by Rating:
 * +------+-----+
 * |rating|count|
 * +------+-----+
 * |     1| 6110|
 * |     2|11370|
 * |     3|27145|
 * |     4|34174|
 * |     5|21201|
 * +------+-----+
 */

package com.delvinlow.sparkpractices

import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}

object MoviesRatingsCounter { // Learning Point: For Spark, not extend scala.App which may not work correctly in Spark. Define main function

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Learning Point: A sparkContext must be defined inside main, but we do not use low-level sparkContext anymore,
    // as the Entry point to all functionality in Spark is now SparkSession object. Hence, use SparkSession.builder
    // to create basic SparkSession
    val spark = SparkSession // Learning Point: SparkSession is a singleton object so do not use new
      .builder()
      .appName("RatingsCounterDF")
      .config("spark.master", "local[*]") // Learning Point: for local only means spark will run as single JVM, remove this for production
      .getOrCreate()

    // Learning Point: If want to use RDD to map before conversion to DataFrame, uncomment lines below
    // import spark.implicits._ // Learning Point: import spark.implicits._ required for implicit conversions like RDD to DataFrames
    // val ratings = spark.sparkContext.textFile("./ml-100k/u.data").map(x => x.toString.split("\t")(2))
    // val moviesDF = ratings.toDF()

    val moviesRatingsSchema = new StructType()
      .add("moviesID", IntegerType, true)
      .add("usersID", IntegerType, true)
      .add("rating", IntegerType, true)
      .add("timestamp", LongType, true)

    val moviesRatings = spark.read
      .format("csv")
      .schema(moviesRatingsSchema)
      .option("delimiter", "\t")
      .load("./ml-100k/u.data")

    println(f"Total Count: ${moviesRatings.count()}")
    moviesRatings.show()

    println(f"Count by Rating:")
    moviesRatings.groupBy("rating").count().sort("rating").show()
  }
}
