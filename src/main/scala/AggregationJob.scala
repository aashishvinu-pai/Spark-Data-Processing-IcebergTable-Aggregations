import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

object AggregationJob {
  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("NYC Taxi Ingestion")
      .master("local[*]")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .config("spark.sql.catalog.spark_catalog.type", "hadoop")
      .config("spark.sql.catalog.spark_catalog.warehouse", "/home/aashishvinu/tasks/simple_scala_spark_job/data/warehouse")
      .getOrCreate()

    import spark.implicits._

    try {
      val rawTable = "spark_catalog.default.nyc_taxi_trips_raw"

      if (!spark.catalog.tableExists(rawTable)) {
        logger.warn("Raw table does not exist yet. Please run IngestionJob first.")
        return
      }

      val rawDF = spark.table(rawTable)
      val rawCount = rawDF.count()
      logger.info(s"Records in raw table: $rawCount")

      if (rawCount == 0) {
        logger.info("No data in raw table. Nothing to aggregate.")
        return
      }

      // Find the latest processed date in the summary table
      val summaryTable = "spark_catalog.default.nyc_taxi_daily_summary"
      val latestProcessedDate = if (spark.catalog.tableExists(summaryTable)) {
        spark.table(summaryTable)
          .agg(max("pickup_date"))
          .collect()
          .headOption
          .flatMap(row => Option(row.getDate(0)))
          .orNull
      } else null

      // Incremental processing
      val incrementalDF = if (latestProcessedDate != null) {
        rawDF.filter($"pickup_date" > lit(latestProcessedDate))
      } else {
        rawDF
      }

      val incCount = incrementalDF.count()
      logger.info(s"Incremental records to process: $incCount")

      if (incCount == 0) {
        logger.info("No new data to process.")
        return
      }

      // ──────────────────────────────────────────────────────────────
      // A. Daily Summary – append (create table automatically on first run)
      // ──────────────────────────────────────────────────────────────
      val dailySummary = incrementalDF.groupBy("pickup_date")
        .agg(
          count("*").as("total_trips"),
          sum("passenger_count").as("total_passengers"),
          sum("fare_amount").as("total_fare"),
          avg("trip_distance").as("avg_distance"),
          avg("trip_duration_minutes").as("avg_duration_min"),
          avg("average_speed_mph").as("avg_speed_mph")
        )

      dailySummary.write
        .format("iceberg")
        .mode("append")
        .saveAsTable("spark_catalog.default.nyc_taxi_daily_summary")

      logger.info(s"Daily summary updated → ${dailySummary.count()} new rows processed")

      // ──────────────────────────────────────────────────────────────
      // B. Hourly Patterns – append
      // ──────────────────────────────────────────────────────────────
      val hourlyPatterns = incrementalDF.groupBy("pickup_date", "pickup_hour")
        .agg(
          count("*").as("trip_count"),
          avg("fare_amount").as("avg_fare"),
          avg("trip_distance").as("avg_distance"),
          avg("trip_duration_minutes").as("avg_duration")
        )

      hourlyPatterns.write
        .format("iceberg")
        .mode("append")
        .saveAsTable("spark_catalog.default.nyc_taxi_hourly_patterns")

      // ──────────────────────────────────────────────────────────────
      // C. Top 100 location pairs – full overwrite (small table)
      // ──────────────────────────────────────────────────────────────
      val topLocations = incrementalDF
        .groupBy("pickup_location_id", "dropoff_location_id")
        .agg(
          count("*").as("trip_count"),
          avg("fare_amount").as("avg_fare"),
          avg("trip_distance").as("avg_distance")
        )
        .orderBy(desc("trip_count"))
        .limit(100)

      topLocations.write
        .format("iceberg")
        .mode("overwrite")
        .saveAsTable("spark_catalog.default.nyc_taxi_top_locations")

      // Quick validation sample
      logger.info("Latest daily summary (top 5 newest days):")
      spark.table("spark_catalog.default.nyc_taxi_daily_summary")
        .orderBy(desc("pickup_date"))
        .show(5, truncate = false)

    } catch {
      case e: Exception =>
        logger.error("Aggregation job failed", e)
    } finally {
      spark.stop()
    }
  }
}