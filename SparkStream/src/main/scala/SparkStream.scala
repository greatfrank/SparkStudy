import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStream {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

    /**
     * 定义spark的配置来利用所有的资源。同时设置应用的名称
     */
    val conf = new SparkConf().setMaster("local[*]").setAppName("TerminalWordCount")


    /**
     * 利用上面的配置创建spark streaming上下文。设置批处理batch的间隔为 1 秒
     */
    val second = 1
    val ssc = new StreamingContext(conf, Seconds(second))

    /**
     * 因为是流数据处理，所以需要实时的接收数据。所以这里就利用一个简便的方式：
     * 构建一个简便的本地网络环境，利用端口9999来接收数据。
     * 接收数据的方式后面会交给 netcat 这个工具来实现
     */
    val lines = ssc.socketTextStream("localhost", 9999)

    /**
     * 将接收到的文字用空格分隔，然后通过key的对来reduce最后的结果
     * 最后的结果就是统计单词的个数
     */
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    /**
     * 打印出每个RDD的前10个元素
     */
    wordCounts.print()

    /**
     * 开始计算
     */
    ssc.start()

    /**
     * 等待计算的结束
     */
    ssc.awaitTermination()





    //    /////////////////////////////////////////////////


    //    val spark = SparkSession.builder().master("local").getOrCreate()
    //
    //    //    -------------------------------------------------
    //
    //    val mobileDataDF = spark.read.json("src/main/scala/data/mobile")
    //    mobileDataDF.printSchema()
    //
    //    /**
    //     * 可以先定义出来数据的格式schema，这样就可以在读取数据的时候根据schema，将里面的null数据进行处理。
    //     */
    //    val mobileDataSchema = new StructType()
    //      .add("id", StringType, false)
    //      .add("action", StringType, false)
    //      .add("ts", TimestampType, false)
    //
    //    line("generate a count per action type in a ten-second sliding window")
    //
    //    import spark.implicits._
    //
    //    val mobileSSDF = spark.readStream.schema(mobileDataSchema).json("src/main/scala/data/input")
    //    println(mobileSSDF.isStreaming)
    //
    //    val actionCountDF = mobileSSDF.groupBy(window($"ts", "10 minutes"), $"action").count()
    //    val mobileConsoleSQ = actionCountDF.writeStream.format("console").option("truncate", false).outputMode("complete").start()


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
