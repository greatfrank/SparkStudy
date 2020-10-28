import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object Run {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local").setAppName("My App")
    val sc = new SparkContext(conf)

    val lines = sc.parallelize(List("pandas", "i like pandas"))

    lines.foreach(println)



    //    =====================================================
  }
}
