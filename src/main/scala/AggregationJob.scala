import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

object AggregationJob {
  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("NYC Taxi Aggregation - Iceberg")
      .master("local[*]")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.ice_hadoop", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.ice_hadoop.type", "hadoop")
      .config("spark.sql.catalog.ice_hadoop.warehouse", "/home/aashishvinu/tasks/spark_iceberg/spark-warehouse")
      .config("spark.sql.defaultCatalog", "ice_hadoop")
      .getOrCreate()

    import spark.implicits._

    try {
      val rawTable = "nyc_taxi_trips_raw"

      if (!spark.catalog.tableExists(rawTable)) {
        logger.error(s"Raw table '$rawTable' not found in catalog")
        sys.exit(1)
      }

      val rawDF = spark.table(rawTable)
      val dailySummary = rawDF
        .groupBy("pickup_date")
        .agg(
          count("*").as("trip_count"),
          sum("trip_distance").as("total_distance"),
          avg("fare_amount").as("avg_fare"),
          sum("total_amount").as("total_revenue")
        )
        .orderBy("pickup_date")

      dailySummary.show(10, truncate = false)
      val summaryTable = "nyc_taxi_daily_summary"

      if (spark.catalog.tableExists(summaryTable)) {
        logger.info(s"Appending to existing table $summaryTable")
        dailySummary.writeTo(summaryTable).append()
      } else {
        logger.info(s"Creating new Iceberg table $summaryTable")
        dailySummary.writeTo(summaryTable)
          .partitionedBy($"pickup_date")
          .create()
      }

      logger.info("Aggregation completed successfully")

    } catch {
      case e: Throwable =>
        logger.error("Unexpected error in AggregationJob", e)
        sys.exit(1)
    } finally {
      spark.stop()
    }
  }
}