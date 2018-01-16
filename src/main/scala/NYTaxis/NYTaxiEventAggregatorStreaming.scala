package NYTaxis

import java.sql.Timestamp
import java.text.SimpleDateFormat

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.log4j.{Level, Logger}
import kafka.serializer.StringDecoder
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Minutes, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

case class TaxiTripEvent(year: Int, tpep_pickup_datetime: Timestamp,
                         tpep_dropoff_datetime: Timestamp, passenger_count: Int,
                         trip_distance: Float, payment_type: Int, total_amount: Float) extends Serializable


object NYTaxiEventAggregatorStreaming extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  private val conf = new SparkConf()
    .setAppName("NYTaxiEventStreaming")
    .setMaster("local[*]")
    .set("spark.cassandra.connection.host", "10.0.2.15")
    .set("spark.cassandra.connection.keep_alive_ms","900000") //Preserve cassandra connection between windows batches

  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc, Minutes(10)) //Get Streaming context with 10min window

  //Create Keyspace and Tables in Cassandra
  val cassandraConnector = CassandraConnector.apply(sc.getConf)
  cassandraConnector.withSessionDo(session => CassandraCQL.createKeySpaceAndTables(session))

  //Get the Input DS Stream from kafka queue
  val kafkaParams = Map[String, String]("metadata.broker.list" -> "localhost:9092")
  val dsKafka = KafkaUtils
    .createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set("taxiEventsFlow"))

  //Function to get a Row from streaming window RDD and produce a DS
  def createTaxiTrip(tripAtts: Array[String]) = {
    val format = new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss")
    val lpep_pickup_datetime = new Timestamp (format.parse(tripAtts(1)).getTime)
    val lpep_dropoff_datetime = new Timestamp (format.parse(tripAtts(2)).getTime)
    val year = tripAtts(1).takeWhile(c => c != '-')

    TaxiTripEvent(year.trim.toInt, lpep_pickup_datetime, lpep_dropoff_datetime,
      tripAtts(7).trim.toInt, tripAtts(8).trim.toFloat,
      tripAtts(17).trim.toInt, tripAtts(16).trim.toFloat)
  }

  //Process each RDD by time window and saving in HDFS and Cassandra tables
  dsKafka.map(_._2)
    .foreachRDD(rddStringW => {

      val spark = SparkSession
        .builder
        .config(rddStringW.sparkContext.getConf)
        .getOrCreate()

      import spark.implicits._

      //Create DataSet with a TaxiTripEvent per Row
      val dsTripsW = rddStringW.map(_.split(','))
        .map(trip => createTaxiTrip(trip))
        .toDS()

      //Get counter and check if there are trips saved in DS
      val tripsCounterPerWindow = dsTripsW.count()
      if (tripsCounterPerWindow != 0) {

        //Save raw data into HDFS
        dsTripsW.write
          .mode(SaveMode.Append)
          .parquet("hdfs://10.0.2.15/taxitrips")

        //Get the most used type of payment in time window
        val dfGroupByPayment = dsTripsW.groupBy("payment_type").count()
        val mostUsedPaymentCounter = dfGroupByPayment.agg(max(dfGroupByPayment("count"))).first().getLong(0)
        val mostUsedPayment = dfGroupByPayment.filter($"count" >= mostUsedPaymentCounter).first().getInt(0)

        //Get TS of the last event processed in time window
        val lastEvent = dsTripsW.agg(max(dsTripsW("tpep_dropoff_datetime"))).first().getTimestamp(0)

        //Get a Row with aggregated trip values in time window
        val rowAggValues = dsTripsW.agg(sum(dsTripsW("trip_distance")),
          sum(dsTripsW("total_amount")),
          sum(dsTripsW("passenger_count")))
          .first()

        //Save aggregated values into Cassandra
        val cassandraConnectorWorker = CassandraConnector.apply(rddStringW.sparkContext.getConf)
        cassandraConnectorWorker.withSessionDo(session => CassandraCQL.insert(session, mostUsedPayment,
          rowAggValues.getDouble(0), rowAggValues.getDouble(1), rowAggValues.getLong(2),
          tripsCounterPerWindow, lastEvent))
      }
    })

  ssc.start()
  ssc.awaitTermination()


}