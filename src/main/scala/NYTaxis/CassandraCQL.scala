package NYTaxis

import java.sql.Timestamp

import com.datastax.driver.core.Session

object CassandraCQL {

  def createKeySpaceAndTables (session: Session) = {

    session.execute("CREATE KEYSPACE IF NOT EXISTS NYTaxiEvents WITH replication " +
      "= {'class':'SimpleStrategy', 'replication_factor':1};")

    session.execute(
      "CREATE TABLE if not exists NYTaxiEvents.trips_bydistance (" +
        "trips_distance DOUBLE," +
        "tripsCount INT," +
        "ts TIMESTAMP," +
        "PRIMARY KEY (trips_distance, ts)" +
        ") WITH CLUSTERING ORDER BY (ts DESC);")

    session.execute(
      "CREATE TABLE if not exists NYTaxiEvents.trips_bypayment_mode (" +
        "most_used_payment INT," +
        "tripsCount INT," +
        "ts TIMESTAMP," +
        "PRIMARY KEY (most_used_payment, ts)" +
        ") WITH CLUSTERING ORDER BY (ts DESC);")

    session.execute(
      "CREATE TABLE if not exists NYTaxiEvents.trips_bypayment_amount (" +
        "trips_paymentamount DOUBLE," +
        "tripsCount INT," +
        "ts TIMESTAMP," +
        "PRIMARY KEY (trips_paymentamount, ts)" +
        ") WITH CLUSTERING ORDER BY (ts DESC);")

    session.execute(
      "CREATE TABLE if not exists NYTaxiEvents.trips_bypassenger_number (" +
        "passenger_number BIGINT," +
        "tripsCount INT," +
        "ts TIMESTAMP," +
        "PRIMARY KEY (passenger_number, ts)" +
        ") WITH CLUSTERING ORDER BY (ts DESC);")

  }

  def insert (session: Session, most_used_payment: Int, trips_distance: Double, trips_paymentamount: Double,
              passenger_number: Long, tripsCount: Long, ts: Timestamp) = {

    session.execute(
      "INSERT INTO NYTaxiEvents.trips_bydistance (trips_distance, tripsCount, ts) " +
        "VALUES (" +
        trips_distance + "," +
        tripsCount + "," +
        ts.getTime + ");")

    session.execute(
      "INSERT INTO NYTaxiEvents.trips_bypayment_mode(most_used_payment, tripsCount, ts) " +
        "VALUES (" +
        most_used_payment + "," +
        tripsCount + "," +
        ts.getTime + ");")

    session.execute(
      "INSERT INTO NYTaxiEvents.trips_bypayment_amount (trips_paymentamount, tripsCount, ts) " +
        "VALUES (" +
        trips_paymentamount + "," +
        tripsCount + "," +
        ts.getTime + ");")

    session.execute(
      "INSERT INTO NYTaxiEvents.trips_bypassenger_number (passenger_number, tripsCount, ts) " +
        "VALUES (" +
        passenger_number + "," +
        tripsCount + "," +
        ts.getTime + ");")
  }
}
