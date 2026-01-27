import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

object IngestionJob {
  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("NYC Taxi Ingestion - Iceberg")
      .master("local[*]")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.ice_hadoop", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.ice_hadoop.type", "hadoop")
      .config("spark.sql.catalog.ice_hadoop.warehouse", "/home/aashishvinu/tasks/spark_iceberg/spark-warehouse")
      .config("spark.sql.defaultCatalog", "ice_hadoop")
      .getOrCreate()

    import spark.implicits._

    try {
      val inputDir = "/home/aashishvinu/tasks/spark_iceberg/data/input"

      logger.info(s"Reading all Parquet files from directory: $inputDir")

      val rawDF = spark.read
        .option("mergeSchema", "true")         
        .parquet(inputDir)                       

      val rawCount = rawDF.count()
      logger.info(s"Total raw records found: $rawCount")

      if (rawCount == 0) {
        logger.warn("No records found in input directory → exiting")
        return
      }

      val renamedDF = rawDF
        .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
        .withColumnRenamed("PULocationID", "pickup_location_id")
        .withColumnRenamed("DOLocationID", "dropoff_location_id")

      val nonNullDF = renamedDF.na.drop(Seq("pickup_datetime", "dropoff_datetime", "trip_distance", "fare_amount"))

      val filteredDF = nonNullDF.filter(
        $"trip_distance" > 0 &&
        $"fare_amount" > 0 &&
        $"total_amount" > 0
      )

      val enhancedDF = filteredDF
        .withColumn("trip_duration_minutes", (unix_timestamp($"dropoff_datetime") - unix_timestamp($"pickup_datetime")) / 60)
        .withColumn("average_speed_mph", when($"trip_duration_minutes" > 0, $"trip_distance" / ($"trip_duration_minutes" / 60)).otherwise(null))
        .withColumn("pickup_date", to_date($"pickup_datetime"))
        .withColumn("pickup_hour", hour($"pickup_datetime"))
        .filter($"trip_duration_minutes" > 0)

      val recordCount = enhancedDF.count()
      if (recordCount == 0) {
        logger.warn("No valid records after filtering → no write performed")
        return
      }

      val tableName = "nyc_taxi_trips_raw"
      val tableExists = spark.catalog.tableExists(tableName)
      val writer = enhancedDF.writeTo(tableName)

      if (!tableExists) {
        writer.partitionedBy($"pickup_date").create()
        logger.info("Created new Iceberg table: " + tableName)
      } else {
        writer.append()
        logger.info("Appended to existing Iceberg table: " + tableName)
      }

      spark.catalog.refreshTable(tableName)

      logger.info("Ingestion completed successfully")

    } catch {
      case e: Throwable =>
        logger.error("Exception during ingestion", e)
        sys.exit(1)
    } finally {
      spark.stop()
    }
  }
}