package NYTaxis

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession


object NYAnalyticsExtractor extends App {

  val log = Logger.getLogger("\nPerformance difference")

  val spark = SparkSession
    .builder
    .master("local[1]")
    .appName("NYTaxiTrips")
    .config("spark.cassandra.connection.host", "10.0.2.15")
    .config("spark.eventLog.enabled", "true")
    .getOrCreate()

  //Get aggregated data from Cassandra
  val dfOlap = spark
    .read
    .format("org.apache.spark.sql.cassandra")
    .options(Map("table" -> "trips_bytime", "keyspace" -> "nytaxievents"))
    .load()

  //Get raw data from hdfs
  val dfRaw = spark
    .read
    .parquet("hdfs://10.0.2.15/taxitrips")

  val startRaw=System.nanoTime()

  dfRaw.createOrReplaceTempView("rawData")
  spark.time(spark.sql("SELECT AVG(passenger_count) FROM rawData " +
    "WHERE tpep_dropoff_datetime BETWEEN '2017-06-01 00:00:00' AND '2017-06-30 23:59:59'").show())

  val finishRawStartOlap=System.nanoTime()

  dfOlap.createOrReplaceTempView("olapData")
  spark.time(spark.sql("SELECT SUM(passenger_number)/SUM(tripsCount) FROM olapData " +
    "WHERE monthyear=62017 " +
    "AND ts BETWEEN '2017-06-01 00:00:00' AND '2017-06-30 23:59:59'").show())

  val finishOlap=System.nanoTime()

  log.info("Olap query is " + (finishRawStartOlap-startRaw)/(finishOlap-finishRawStartOlap) + " times faster than " +
    "Raw query")


}
