import org.apache.spark.sql.SparkSession

object SparkSQLAdvanced {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._
    //    --------------------------
    val flight_summary = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/main/scala/data/flights/flight-summary.csv")
    println(flight_summary.count())

    //    ==============================================
  }
}
