import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory
import ai.prevalent.sdspecore.sparkbase.table.iceberg.{SDSIcebergReader, SDSIcebergWriter}

object AggregationJob {
  
  private val logger = LoggerFactory.getLogger(getClass)

  val RAW_TABLE = "default.nyc_taxi_trips_raw"
  val DAILY_TABLE = "default.nyc_taxi_daily_summary"
  val HOURLY_TABLE = "default.nyc_taxi_hourly_patterns"
  val TOP_LOC_TABLE = "default.nyc_taxi_top_locations"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("NYC Taxi - Daily + Hourly + Top Locations")
      .master("local[*]")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.iceberg_catalog.type", "hadoop")
      .config("spark.sql.catalog.iceberg_catalog.warehouse", "/home/aashishvinu/tasks/spark_iceberg/spark-warehouse")
      .config("spark.sql.defaultCatalog", "iceberg_catalog")
      .getOrCreate()

    import spark.implicits._

    val reader = new SDSIcebergReader(spark)
    val writer = new SDSIcebergWriter(spark)

    try {

      if (!spark.catalog.tableExists(RAW_TABLE)) {
        logger.error("Raw table not found! Please run the ingestion job first.")
        System.exit(1)
      }

      val rawData = reader.read(RAW_TABLE)
      logger.info(s"Loaded ${rawData.count()} rows from raw table")

      val lastDate = if (spark.catalog.tableExists(DAILY_TABLE)) {
        reader.read(DAILY_TABLE)
          .agg(max("pickup_date"))
          .head()
          .getString(0)
      } else {
        "1970-01-01"
      }

      logger.info(s"Last processed date was: $lastDate")

      val newData = rawData.filter($"pickup_date" > lit(lastDate))
      val newCount = newData.count()

      if (newCount == 0) {
        logger.info("No new trips to process :) Exiting.")
        return
      }

      logger.info(s"Found $newCount new trips to process!")

      // --- Daily summary ---
      val daily = newData.groupBy("pickup_date").agg(
        count("*").as("total_trips"),
        sum("passenger_count").cast("long").as("total_passengers"),
        sum("total_amount").as("total_fare"),
        avg("trip_distance").as("avg_distance"),
        avg("trip_duration_minutes").as("avg_duration"),
        avg("average_speed_mph").as("avg_speed")
      ).orderBy("pickup_date")

      writer.append(daily, DAILY_TABLE, Array($"pickup_date"))
      logger.info(s"Wrote/updated daily summary (${daily.count()} rows)")

      // --- Hourly summary ---
      val hourly = newData.groupBy("pickup_date", "pickup_hour").agg(
        count("*").as("trip_count"),
        avg("total_amount").as("avg_fare"),
        avg("trip_distance").as("avg_distance"),
        avg("trip_duration_minutes").as("avg_duration")
      ).orderBy("pickup_date", "pickup_hour")

      writer.append(hourly, HOURLY_TABLE, Array($"pickup_date"))
      logger.info(s"Wrote/updated hourly patterns (${hourly.count()} rows)")

      // --- Top 100 pickup-dropoff pairs ---
      val top100 = newData.groupBy("pickup_location_id", "dropoff_location_id").agg(
        count("*").as("trip_count"),
        avg("total_amount").as("avg_fare"),
        avg("trip_distance").as("avg_distance")
      ).orderBy(desc("trip_count"))
       .limit(100)

      writer.overwritePartition(top100, TOP_LOC_TABLE)
      logger.info(s"Updated top 100 locations table (${top100.count()} rows)")

      logger.info("All done! :)")

    } catch {
      case e: Exception =>
        logger.error("Something went wrong", e)
        System.exit(1)
    } finally {
      spark.stop()
    }
  }
}