import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object Run {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    //    set configuration info
    val conf = new SparkConf().setMaster("local[*]").setAppName("My App")
    //    create a instance of SparkContext
    val sc = new SparkContext(conf)
    //    load local file and create a RDD
    val linesRDD = sc.textFile("src/main/scala/data/README.md")
    //    调用filter这个API，创建出一个新的RDD，名为 sparkLines
    val sparkLines = linesRDD.filter(line => line.contains("Spark"))
    //    打印RDD中的第一个元素
    println(sparkLines.first())
    //    统计RDD中元素的个数
    println(sparkLines.count())


    //============================
  }
}
