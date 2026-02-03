# NYC Taxi Iceberg Pipeline

End-to-end Spark + Apache Iceberg pipeline that ingests NYC Yellow Taxi trip data, performs cleaning and enrichment, and creates aggregated summary tables using incremental processing.

## Tables Created

| Table name                        | Purpose                                      | Partitioned by    | Write mode   |
|-----------------------------------|----------------------------------------------|-------------------|--------------|
| `nyc_taxi_trips_raw`              | Cleaned and enriched raw trip records        | `pickup_date`     | append       |
| `nyc_taxi_daily_summary`          | Daily aggregates (trips, passengers, fare…)  | `pickup_date`     | append       |
| `nyc_taxi_hourly_patterns`        | Hourly trip counts and averages              | `pickup_date`     | append       |
| `nyc_taxi_top_locations`          | Top 100 pickup–dropoff location pairs by count | none            | overwrite    |

## Folder Structure

```
├── data/
│   └── input/                    # Put yellow_tripdata_*.parquet files here
├── spark-warehouse/              # Iceberg warehouse (auto-created)
├── src/
│   └── main/
│       └── scala/                # Main application code
├── project/
│   ├── build.properties
│   └── plugins.sbt
├── build.sbt
└── README.md
```

- `data/input/`  
  Place raw NYC Yellow Taxi Parquet files here before running the ingestion job.

- `spark-warehouse/`  
  Iceberg metadata and data files are written here using Hadoop catalog. Contains `default/` namespace with one subfolder per table.

- `src/main/scala/`  
  Contains the two main jobs:  
  - `IngestionJob.scala` — reads Parquet files, cleans data, adds derived columns, writes to raw table  
  - `AggregationJob.scala` — incremental aggregations and top locations

## What Happens Inside Each Job

### 1. IngestionJob.scala — Raw Data Ingestion

**Main steps performed:**

1. Read all Parquet files from `data/input/` with schema merging enabled  
   → `spark.read.option("mergeSchema", "true").parquet(inputDir)`

2. **Normalization & cleaning**:
   - Convert all column names to lowercase
   - Rename columns for consistency:
     - `tpep_pickup_datetime` → `pickup_datetime`
     - `tpep_dropoff_datetime` → `dropoff_datetime`
     - `pulocationid` → `pickup_location_id`
     - `dolocationid` → `dropoff_location_id`
   - Drop rows where critical columns are null:
     - `pickup_datetime`, `dropoff_datetime`, `trip_distance`, `total_amount`
   - Filter invalid records:
     - `trip_distance > 0`
     - `total_amount > 0`
     - `fare_amount > 0`

3. **Enrichment / Derived columns**:
   - `trip_duration_minutes` = `(unix_timestamp(dropoff) - unix_timestamp(pickup)) / 60.0`
   - `average_speed_mph` = `trip_distance / (trip_duration_minutes / 60.0)` (null if duration ≤ 0)
   - `pickup_date` = `to_date(pickup_datetime)`
   - `pickup_hour` = `hour(pickup_datetime)`
   - Final filter: `trip_duration_minutes > 0`

4. Write cleaned & enriched DataFrame to Iceberg table `default.nyc_taxi_trips_raw`  
   - Partitioned by `pickup_date`
   - Mode: **append**
   - Uses `SDSIcebergWriter.append(...)`

### 2. AggregationJob.scala — Incremental Aggregations

**Main steps performed:**

1. Read raw table using `SDSIcebergReader.read(...)`

2. **Incremental logic**:
   - Find the last processed date from `nyc_taxi_daily_summary` (or default to 1970-01-01)
   - Filter raw data to only dates > last processed date

3. **Daily summary** (appended to `nyc_taxi_daily_summary`):
   - Group by `pickup_date`
   - Aggregates:
     - `total_trips` = `count(*)`
     - `total_passengers` = `sum(passenger_count)`
     - `total_fare` = `sum(total_amount)`
     - `avg_distance` = `avg(trip_distance)`
     - `avg_duration` = `avg(trip_duration_minutes)`
     - `avg_speed` = `avg(average_speed_mph)`

4. **Hourly patterns** (appended to `nyc_taxi_hourly_patterns`):
   - Group by `pickup_date`, `pickup_hour`
   - Aggregates:
     - `trip_count` = `count(*)`
     - `avg_fare` = `avg(total_amount)`
     - `avg_distance` = `avg(trip_distance)`
     - `avg_duration` = `avg(trip_duration_minutes)`

5. **Top locations** (overwrite `nyc_taxi_top_locations`):
   - Group by `pickup_location_id`, `dropoff_location_id`
   - Aggregates:
     - `trip_count` = `count(*)`
     - `avg_fare` = `avg(total_amount)`
     - `avg_distance` = `avg(trip_distance)`
   - Order by `trip_count` descending → take top 100

6. Write using `SDSIcebergWriter`:
   - Append for daily & hourly
   - Overwrite for top locations

## How to Run Each Job

### 1. Clean Assembly
```bash
sbt clean update compile assembly
```

### 2. Ingestion Job

```bash
spark-submit --class IngestionJob --driver-memory 20g --executor-memory 20g target/scala-2.13/nyc-taxi-iceberg-assembly-1.0.jar 
```

### 3. Aggregation Job

```bash
spark-submit --class AggregationJob --driver-memory 20g --executor-memory 20g target/scala-2.13/nyc-taxi-iceberg-assembly-1.0.jar
```

## Starting Spark Shell (to query tables)

```bash
spark-shell --master "local[*]" \
  --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
  --conf "spark.sql.catalog.iceberg_catalog=org.apache.iceberg.spark.SparkCatalog" \
  --conf "spark.sql.catalog.iceberg_catalog.type=hadoop" \
  --conf "spark.sql.catalog.iceberg_catalog.warehouse=/home/aashishvinu/tasks/spark_iceberg/spark-warehouse" \
  --conf "spark.sql.defaultCatalog=iceberg_catalog" \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.9.0,ch.qos.logback:logback-classic:1.5.12
```

Replace the warehouse path with your actual absolute path.

## Example Queries in Spark Shell

```scala
// List tables
spark.sql("SHOW TABLES").show(false)

// Row counts
spark.sql("SELECT count(*) AS raw_count FROM nyc_taxi_trips_raw").show()
spark.sql("SELECT count(*) AS daily_count FROM nyc_taxi_daily_summary").show()

// Top 10 locations
spark.sql("SELECT * FROM nyc_taxi_top_locations ORDER BY trip_count DESC LIMIT 10").show(false)

// Recent snapshots
spark.sql("SELECT snapshot_id, committed_at FROM nyc_taxi_trips_raw.snapshots ORDER BY committed_at DESC LIMIT 5").show(false)

// Sample raw data
spark.sql("SELECT * FROM nyc_taxi_trips_raw LIMIT 5").show(false)
```


