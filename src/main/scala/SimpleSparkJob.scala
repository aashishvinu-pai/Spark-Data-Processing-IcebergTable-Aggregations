import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SimpleSparkJob {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .appName("SimpleSparkJob")
      .config("spark.hadoop.fs.defaultFS", "file:///")
      .config("spark.hadoop.hadoop.home", "/opt/hadoop") 
      .master("local[*]")
      .getOrCreate()
    

    val inputFilePath = "input.json"  
    val df = spark.read.json(inputFilePath)

    val modifiedDf = df
      .withColumn("age_plus_ten", col("age") + 10)
      .filter(col("age") > 25)

    val outputFilePath = "output.json"  
    modifiedDf.write.json(outputFilePath)
    spark.stop()
  }
}
