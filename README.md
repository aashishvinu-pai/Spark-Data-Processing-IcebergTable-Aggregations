# Simple Scala Spark Job

A minimal Apache Spark example written in Scala. This repository demonstrates a tiny Spark job that reads a JSON file, transforms it, and writes the result back as JSON. The project uses Maven to build and produces a thin JAR that is intended to be run with spark-submit.

Repository details
- Repository: aashishvinu-pai/Simple-Scala-Spark-Job
- Language: Scala
- Main class: `SimpleSparkJob` (top-level object, no package)
- Example input (included): `input.json`
- Example output (produced): `output.json` (Spark will create a directory with part files)

Important versions (from pom.xml)
- Scala: 2.13.15
- Spark: 4.1.1
- Build: Maven

Quick overview of the job (what it does)
- Reads JSON from `input.json`
- Adds a column `age_plus_ten` = `age + 10`
- Filters rows where `age > 25`
- Writes the resulting DataFrame to `output.json` (as Spark JSON output)

Prerequisites
- Java JDK 8 or 11
- Maven 3.x
- Apache Spark (the cluster or local Spark installation that will run `spark-submit`) — this project declares Spark dependencies with scope `provided`, so Spark's runtime must supply the Spark libraries.

Build (using Maven)
1. From the repository root run:
   mvn clean package

2. After a successful build the thin JAR will be available at:
   target/simple-spark-job-1.0-SNAPSHOT.jar

Run with spark-submit (example)
- Local-mode example (uses Spark installed on the machine):
  spark-submit \
    --class SimpleSparkJob \
    --master local[*] \
    target/simple-spark-job-1.0-SNAPSHOT.jar

Notes:
- Because the code uses hard-coded file paths:
  - Input file path: `input.json` (the repository includes a sample `input.json`)
  - Output path: `output.json` — Spark will create a directory named `output.json` with part files and metadata
- If you run spark-submit from a different working directory, either provide absolute paths or copy `input.json` into the working directory.
- The `pom.xml` marks Spark dependencies as `<scope>provided</scope>` so do not expect them in the JAR — rely on the Spark distribution used by `spark-submit`.


Repository contents (relevant)
- `pom.xml` — Maven build configuration, Scala plugin, and Spark dependencies declared as provided
- `src/main/scala/SimpleSparkJob.scala` — main job; uses SparkSession, reads `input.json`, writes `output.json`
- `input.json` — sample input:
  [
    {"name": "John", "age": 30},
    {"name": "Jane", "age": 22},
    {"name": "Alice", "age": 35}
  ]
