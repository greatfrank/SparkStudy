import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStream {

  def main(args: Array[String]): Unit = {

    //    隐藏不必要的日志
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

    //    创建一个sparkSession实例
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._

    //    做这一步是为了下面生成
    //    val mobileDataDF = spark.read.json("src/main/scala/data/mobile")
    //    mobileDataDF.printSchema()

    /**
     * 定义数据的结构，下面的add里面的第一个参数对应的是后面接收到的json文件里的key的名称以及数据类型。而且这些数据都不能为空（nullable=false）
     */
    val mobileDataSchema = new StructType()
      .add("id", StringType, false)
      .add("action", StringType, false)
      .add("ts", TimestampType, false)

    /**
     * 监听目录【src/main/scala/data/input】里的所有json文件。默认的这个目录是空的，一旦里面有了json文件，就会对其进行分析和统计。而且从这个目录下读取的文件的schema就是上面mobileDataSchema所定义的。
     */
    val mobileSSDF = spark.readStream.schema(mobileDataSchema).json("src/main/scala/data/input")

    //    返回 true。因为这次的DStream是一种流式的数据类型
    println(mobileSSDF.isStreaming)

    /**
     * 针对每一种 action 创建一个count计数器。
     * 以 10 秒钟为时间窗口，用数据中的 action 这个key对数据进行分组，然后统计个数count
     */
    val actionCountDF = mobileSSDF.groupBy(window($"ts", "10 seconds"), $"action").count()

    /**
     * 这里定义了输出模式以及数据接收器。
     * 流式的DataFrame调用 writeStream，它会返回一个DataFrameWriter类型的实例。紧接着调用后面的API，比如 format、option、outputMode 等。
     * 而且还定义了 Structured Streaming 引擎用上面定义的actionCountDF，开始递增式的运行算法。
     * 定义了数据的输出方式是以console的方式展示
     * outputMode定义为complete，表示你可以在一个table表格里看到所有的结果。
     * 调用了 start() 函数，会让这个 DataFrameWriter 开始运行。来监听目录【src/main/scala/data/input】里的json文件。另外，start 函数将会返回一个 StreamingQuery 的实例，这就是一个把手，以便后面后面继续执行检索数据的工作。
     * >>> 必须保证目录【src/main/scala/data/input】里面开始的时候是空的！！！
     */
    val mobileConsoleSQ = actionCountDF.writeStream.format("console").option("truncate", false).outputMode("complete").start()

    /**
     * 以下的 status 和 lastProgress 只有在打开了日志后才能看到
     */
    //    status 会显示 query stream 的 当前状态
    println(mobileConsoleSQ.status)
    //    lastProgress 函数提供了一些信息，比如最后一个批次数据的处理开销等
    println(mobileConsoleSQ.lastProgress)

    /**
     * mobileConsoleSQ会开始监听目录里的json文件，并进行统计。直到手动停止。
     */
    mobileConsoleSQ.awaitTermination()




    //    /////////////////////////////////////////////////


    //=====================================================
  }

  def line(title: String = ""): Unit = {
    if (title == "") {
      println("\n-------------------------------------------\n")
    } else {
      println(s"\n------------- ${title} ---------------------\n")
    }
  }

  def doubleLine(title: String = ""): Unit = {
    if (title == "") {
      println("\n=====================================\n")
    } else {
      println(s"\n=========== ${title} ============\n")
    }
  }
}
