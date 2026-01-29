import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory
import ai.prevalent.sdspecore.sparkbase.table.iceberg.{SDSIcebergReader, SDSIcebergWriter}

object IngestionJob {
  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("NYC Taxi Ingestion - Iceberg with SDS")
      .master("local[*]")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.iceberg_catalog.type", "hadoop")
      .config("spark.sql.catalog.iceberg_catalog.warehouse", "/home/aashishvinu/tasks/spark_iceberg/spark-warehouse")
      .config("spark.sql.defaultCatalog", "iceberg_catalog")
      .getOrCreate()

    import spark.implicits._

    try {
      val inputDir = "/home/aashishvinu/tasks/spark_iceberg/data/input"
      logger.info(s"Starting ingestion job - reading from: $inputDir")

      val rawDF = spark.read
        .option("mergeSchema", "true")
        .parquet(inputDir)

      val rawCount = rawDF.count()
      logger.info(s"Raw records loaded: $rawCount")
      if (rawCount == 0) {
        logger.warn("No records found - exiting")
        return
      }

      val renamedDF = rawDF
        .toDF(rawDF.columns.map(_.toLowerCase): _*)
        .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
        .withColumnRenamed("pulocationid", "pickup_location_id")
        .withColumnRenamed("dolocationid", "dropoff_location_id")

      val nonNullDF = renamedDF.na.drop(Seq("pickup_datetime", "dropoff_datetime", "trip_distance", "total_amount"))
      val filteredDF = nonNullDF.filter($"trip_distance" > 0 && $"total_amount" > 0 && $"fare_amount" > 0)

      val enhancedDF = filteredDF
        .withColumn("trip_duration_minutes", (unix_timestamp($"dropoff_datetime") - unix_timestamp($"pickup_datetime")) / 60.0)
        .withColumn("average_speed_mph",
          when($"trip_duration_minutes" > 0, $"trip_distance" / ($"trip_duration_minutes" / 60.0))
            .otherwise(lit(null))
        )
        .withColumn("pickup_date", to_date($"pickup_datetime"))
        .withColumn("pickup_hour", hour($"pickup_datetime"))
        .filter($"trip_duration_minutes" > 0)

      val finalCount = enhancedDF.count()
      if (finalCount == 0) {
        logger.warn("No valid rows to write - exiting")
        return
      }

      val tableName = "default.nyc_taxi_trips_raw" 

      logger.info(s"Writing $finalCount records to table: $tableName")

      val writer = new SDSIcebergWriter(spark)
      writer.append(
        df = enhancedDF,
        tableName = tableName,
        partitionCols = Array($"pickup_date")
      )

      logger.info("Ingestion completed successfully")

    } catch {
      case e: Throwable =>
        logger.error("Unexpected error during ingestion", e)
        sys.exit(1)
    } finally {
      spark.stop()
    }
  }
}