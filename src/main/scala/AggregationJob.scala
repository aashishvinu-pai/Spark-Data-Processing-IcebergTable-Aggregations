import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory
import ai.prevalent.sdspecore.sparkbase.table.iceberg.{SDSIcebergReader, SDSIcebergWriter}

object AggregationJob {
  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("NYC Taxi Aggregation - Iceberg with SDS")
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
      val rawTable = "iceberg_catalog.default.nyc_taxi_trips_raw"
 if (!spark.catalog.tableExists(rawTable)) {
        logger.error(s"Source table '$rawTable' does not exist. Run ingestion first.")
        sys.exit(1)
      }

      val rawDF = spark.table(rawTable)
      val rawCount = rawDF.count()
      logger.info(s"Read $rawCount records from raw table")

      val summaryTable = "iceberg_catalog.default.nyc_taxi_daily_summary"
      val lastProcessedDate = if (spark.catalog.tableExists(summaryTable)) {
        spark.table(summaryTable)
          .agg(max("pickup_date").as("max_date"))
          .selectExpr("CAST(max_date AS STRING)")
          .as[String]
          .take(1)
          .headOption
          .getOrElse("1970-01-01")
      } else {
        logger.info("No previous summary table found → processing all data")
        "1970-01-01"
      }

      logger.info(s"Last processed date: $lastProcessedDate → filtering newer data only")

      logger.info(s"Last processed date in summary: $lastProcessedDate → processing newer data")

      val incrementalDF = rawDF.filter($"pickup_date" > lit(lastProcessedDate))
      val incCount = incrementalDF.count()

      if (incCount == 0) {
        logger.info("No new data to process → exiting")
        return
      }

      logger.info(s"Incremental records to process: $incCount (pickup_date > $lastProcessedDate)")

      val dailySummary = incrementalDF.groupBy("pickup_date").agg(
        count("*").as("total_trips"),
        sum("passenger_count").cast("long").as("total_passengers"),   
        sum("total_amount").as("total_fare_amount"),                 
        avg("trip_distance").as("avg_trip_distance"),
        avg("trip_duration_minutes").as("avg_trip_duration"),
        avg("average_speed_mph").as("avg_speed")
      ).orderBy("pickup_date")

      val dailyCount = dailySummary.count()
      logger.info(s"Computed $dailyCount daily summary rows")

      if (spark.catalog.tableExists(summaryTable)) {
        dailySummary.writeTo(summaryTable).append()

        logger.info(s"Appended to $summaryTable")
      } else {
        dailySummary.writeTo(summaryTable)
          .partitionedBy($"pickup_date")
          .create()

        logger.info(s"Created partitioned table $summaryTable")
      }

      val hourlyPatterns = incrementalDF.groupBy("pickup_date", "pickup_hour").agg(
        count("*").as("trip_count"),
        avg("total_amount").as("avg_fare"),
        avg("trip_distance").as("avg_distance"),
        avg("trip_duration_minutes").as("avg_duration")
      ).orderBy("pickup_date", "pickup_hour")

      val hourlyTable = "iceberg_catalog.default.nyc_taxi_hourly_patterns"
      val hourlyCount = hourlyPatterns.count()
      logger.info(s"Computed $hourlyCount hourly pattern rows")

      if (spark.catalog.tableExists(hourlyTable)) {
        hourlyPatterns.writeTo(hourlyTable).append()
      } else {
        hourlyPatterns.writeTo(hourlyTable)
          .partitionedBy($"pickup_date")
          .create()
      }

      val topLocations = incrementalDF.groupBy("pickup_location_id", "dropoff_location_id").agg(
        count("*").as("trip_count"),
        avg("total_amount").as("avg_fare"),
        avg("trip_distance").as("avg_distance")
      ).orderBy(desc("trip_count"))
       .limit(100)

      val topLocTable = "iceberg_catalog.default.nyc_taxi_top_locations"
      val topCount = topLocations.count()
      logger.info(s"Computed top 100 locations ($topCount rows)")
      topLocations.writeTo(topLocTable)
        .createOrReplace()

      logger.info("All aggregations completed successfully")

    } catch {
      case e: Throwable =>
        logger.error("Error in aggregation job", e)
        sys.exit(1)
    } finally {
      spark.stop()
    }
  }
}