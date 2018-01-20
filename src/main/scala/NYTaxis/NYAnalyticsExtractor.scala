package NYTaxis

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object NYAnalyticsExtractor extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

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

  dfRaw.createOrReplaceTempView("rawData")

  spark.time(spark.sql("SELECT AVG(trip_distance) FROM rawData " +
    "WHERE tpep_dropoff_datetime BETWEEN '2017-05-21 20:20:00' AND '2017-05-21 22:10:00'").show())

  dfOlap.createOrReplaceTempView("olapData")

  spark.time(spark.sql("SELECT SUM(trips_distance)/SUM(tripsCount) FROM olapData " +
    "WHERE monthyear=52017 " +
    "AND ts BETWEEN '2017-05-21 20:20:00' AND '2017-05-21 22:10:00'").show())

}
