package NYTaxis

import java.sql.Timestamp
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.datastax.driver.core.Session

object CassandraCQL {

  def createKeySpaceAndTables (session: Session) = {

    session.execute("CREATE KEYSPACE IF NOT EXISTS NYTaxiEvents WITH replication " +
      "= {'class':'SimpleStrategy', 'replication_factor':1};")

    session.execute(
      "CREATE TABLE if not exists NYTaxiEvents.trips_bytime (" +
        "most_used_payment INT," +
        "trips_distance DOUBLE," +
        "trips_paymentamount DOUBLE," +
        "passenger_number BIGINT," +
        "tripsCount INT," +
        "ts TIMESTAMP," +
        "monthyear INT," +
        "PRIMARY KEY ((monthyear,ts),tripsCount)" +
        ") WITH CLUSTERING ORDER BY (tripsCount DESC);")

  }

  def insert (session: Session, most_used_payment: Int, trips_distance: Double, trips_paymentamount: Double,
              passenger_number: Long, tripsCount: Long, ts: Timestamp) = {

    val tsFormater = DateTimeFormatter.ofPattern("yyyy-MM-dd' 'HH:mm:ss.S")
    val dateFromTs = LocalDate.parse(ts.toString, tsFormater)
    val monthYear= {dateFromTs.getMonthValue.toString + dateFromTs.getYear.toString}.trim.toInt

    session.execute(
      "INSERT INTO NYTaxiEvents.trips_bytime (most_used_payment, trips_distance, trips_paymentamount, " +
        "passenger_number, tripsCount, ts, monthyear) " +
        "VALUES (" +
        most_used_payment + "," +
        trips_distance + "," +
        trips_paymentamount + "," +
        passenger_number + "," +
        tripsCount + "," +
        ts.getTime + "," +
        monthYear + ");")

  }
}
