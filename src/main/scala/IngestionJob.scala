import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

object IngestionJob {
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
      val inputPath = "/home/aashishvinu/tasks/simple_scala_spark_job/data/input/yellow_tripdata_2025-02.parquet"
      println("before issues")

      val rawDF = spark.read
        .option("mergeSchema", "true")
        .option("pathGlobFilter", "*.parquet")
        .parquet(inputPath)

      logger.info(s"Records read from source: ${rawDF.count()}")

      val renamedDF = rawDF
        .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
        .withColumnRenamed("PULocationID", "pickup_location_id")
        .withColumnRenamed("DOLocationID", "dropoff_location_id")
        .withColumnRenamed("passenger_count", "passenger_count")
        .withColumnRenamed("trip_distance", "trip_distance")
        .withColumnRenamed("fare_amount", "fare_amount")
        .withColumnRenamed("total_amount", "total_amount")

      val nonNullDF = renamedDF.na.drop(Seq("pickup_datetime", "dropoff_datetime", "trip_distance", "fare_amount"))
      val filteredDF = nonNullDF.filter($"trip_distance" > 0 && $"fare_amount" > 0 && $"total_amount" > 0)

      val enhancedDF = filteredDF
        .withColumn("trip_duration_minutes", (unix_timestamp($"dropoff_datetime") - unix_timestamp($"pickup_datetime")) / 60)
        .withColumn("average_speed_mph", when($"trip_duration_minutes" > 0, $"trip_distance" / ($"trip_duration_minutes" / 60)).otherwise(null))
        .withColumn("pickup_date", to_date($"pickup_datetime"))
        .withColumn("pickup_hour", hour($"pickup_datetime"))
        .filter($"trip_duration_minutes" > 0)

      val recordCount = enhancedDF.count()
      logger.info(s"Records after cleaning & transformation: $recordCount")

      if (recordCount == 0) {
        logger.error("No valid records after cleaning → stopping")
        return 
      }

      val tableName = "spark_catalog.default.nyc_taxi_trips_raw"
      val writer = enhancedDF.writeTo(tableName)

      if (!spark.catalog.tableExists(tableName)) {
        writer
          .partitionedBy($"pickup_date")
          .create()
        logger.info("Created new Iceberg table: nyc_taxi_trips_raw")
      } else {
        writer.append()
        logger.info("Appended to existing Iceberg table")
      }

      spark.catalog.refreshTable(tableName)

      logger.info("Sample from source after processing:")
      enhancedDF.show(10, truncate = false)

      println("testtttt")
      logger.info("Sample from Iceberg table:")
      spark.table(tableName).show(10, truncate = false)

    } catch {
      case e: Exception =>
        logger.error("Ingestion failed", e)
    } finally {
      spark.stop()
    }
  }
}