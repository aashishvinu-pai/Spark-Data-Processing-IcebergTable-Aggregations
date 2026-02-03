# NYC Taxi Trips - Spark + Apache Iceberg Pipeline

End-to-end ETL & analytics pipeline processing **NYC Yellow Taxi Trip Records** using **Apache Spark** and **Apache Iceberg** with partitioned tables, incremental processing, and aggregations.

## Project Overview

This project demonstrates a production-like data lake setup using:

- **Apache Spark** (3.5.x) for distributed processing
- **Apache Iceberg** as the table format (Hadoop catalog, local warehouse)
- Custom SDS utilities (`SDSIcebergReader`, `SDSIcebergWriter`) for reading/writing
- Incremental daily/hourly aggregations + top locations analysis
- Schema evolution support & compaction

### Tables Created

| Table Name                        | Description                                  | Partitioned by     | Write Mode     |
|-----------------------------------|----------------------------------------------|--------------------|----------------|
| `nyc_taxi_trips_raw`              | Cleaned & enriched raw trip records          | `pickup_date`      | Append         |
| `nyc_taxi_daily_summary`          | Daily aggregates (trips, passengers, fare…)  | `pickup_date`      | Append         |
| `nyc_taxi_hourly_patterns`        | Hourly trip patterns                         | `pickup_date`      | Append         |
| `nyc_taxi_top_locations`          | Top 100 pickup-dropoff location pairs        | (no partitioning)  | Overwrite      |

## Folder Structure

```
nyc-taxi-iceberg/
├── data/
│   └── input/                    ← put your yellow_tripdata_*.parquet files here
├── spark-warehouse/              ← Iceberg warehouse (auto-created)
├── src/
│   └── main/
│       └── scala/
│           ├── IngestionJob.scala
│           └── AggregationJob.scala
├── project/
│   ├── build.properties
│   └── plugins.sbt
├── build.sbt
└── README.md
```
## Setup

1. **Clone the repository**

   ```bash
   git clone <your-repo-url>
   cd nyc-taxi-iceberg
   ```

2. **Download sample data** (optional – at least 2 months recommended)

   Place Yellow Taxi Parquet files in `data/input/`, e.g.:

   ```
   data/input/
   ├── yellow_tripdata_2023-01.parquet
   └── yellow_tripdata_2023-02.parquet
   ```

3. **Build the project**

   ```bash
   sbt clean assembly
   ```

   → Produces `target/scala-2.13/nyc-taxi-iceberg-assembly-1.0.jar`

## Running the Pipeline

### 1. Ingestion Job (Raw data → Iceberg)

```bash
spark-submit \
  --class IngestionJob \
  --master local[*] \
  target/scala-2.13/nyc-taxi-iceberg-assembly-1.0.jar
```

### 2. Aggregation Job (Incremental summaries)

```bash
spark-submit \
  --class AggregationJob \
  --master local[*] \
  target/scala-2.13/nyc-taxi-iceberg-assembly-1.0.jar
```

## Querying the Tables (spark-shell example)

```bash
spark-shell \
  --master local[*] \
  --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
  --conf "spark.sql.catalog.iceberg_catalog=org.apache.iceberg.spark.SparkCatalog" \
  --conf "spark.sql.catalog.iceberg_catalog.type=hadoop" \
  --conf "spark.sql.catalog.iceberg_catalog.warehouse=file:///absolute/path/to/nyc-taxi-iceberg/spark-warehouse" \
  --conf "spark.sql.defaultCatalog=iceberg_catalog" \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.9.0
```

Then inside the shell:

```scala
spark.sql("SHOW TABLES").show(false)
spark.sql("SELECT * FROM nyc_taxi_top_locations ORDER BY trip_count DESC LIMIT 10").show(false)
spark.sql("SELECT * FROM nyc_taxi_trips_raw.snapshots ORDER BY committed_at DESC").show(false)
```

## Features Demonstrated

- Schema merging across evolving Parquet files (`mergeSchema = true`)
- Partitioned Iceberg tables (`pickup_date`)
- Incremental processing (only new dates)
- Daily + hourly aggregations
- Top-N location pairs (overwrite)
- Time travel via snapshots
- Metadata inspection (`snapshots`, `files`, `manifests`)

## Tech Stack

- Scala 2.13.17
- Spark 3.5.5
- Iceberg 1.9.0 (spark-runtime-3.5)
- sbt 1.12.0
- Hadoop catalog (local filesystem warehouse)
- Parquet files from NYC TLC: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
  
<img width="533" height="264" alt="Screenshot 2026-01-29 131210" src="https://github.com/user-attachments/assets/d7032df4-f84d-40d5-9f19-a304534e3798" />


