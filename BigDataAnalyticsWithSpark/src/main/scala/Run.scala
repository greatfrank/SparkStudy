import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.streaming._

object Run {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

    //    val sc = new SparkContext()
    val config = new SparkConf().setMaster("local[*]").setAppName("big app")
    val sc = new SparkContext(config)

    //    val xs = (1 to 1000).toList
    //    val xsRdd = sc.parallelize(xs)
    //    val evenRdd = xsRdd.filter(_ % 2 == 0)
    //    val count = evenRdd.count()
    //    println(count)
    //
    //    line()
    //
    //    val first = evenRdd.first()
    //    println(first)
    //
    //    val first5 = evenRdd.take(5)
    //    first5.foreach(println)


    //    val rawLogs = sc.textFile("src/main/scala/data/app.log")
    //    val logs = rawLogs.map(line => line.trim.toLowerCase())
    //    //    将RDD缓存进内存
    //    logs.persist()
    //    //    统计日志文件总共的行数
    //    val totalCount = logs.count()
    //    println(totalCount)

    //    val errorLogs = logs.filter { line =>
    //      //      将一行文字用空格划分为列表
    //      val words = line.split(" ")
    //      //      日志文件中，每一行的第二个文字保存着
    //      val logLevel = words(1)
    //      //      注意只有返回true的时候，这一行才会被选择出来
    //      logLevel == "error"
    //    }

    /**
     * 如果不知道日志文件里具体是用一个空格、tab、还是多个空格作为分隔符，那么可以利用通配符来做分割
     */
    //    val errorLogs = logs.filter {
    //      _.split("\\s+")(1).toLowerCase() == "error"
    //    }
    //
    //    line("统计错误日志的条数")
    //    println(errorLogs.count())
    //
    //    line("打印第一条日志信息")
    //    val firstError = errorLogs.first()
    //    println(firstError)
    //
    //    line("打印前3条日志信息")
    //
    //    //    查看前 3 个错误日志的信息
    //    val first3Errors = errorLogs.take(3)
    //    first3Errors.foreach(println)
    //
    //    line("最长的日志行，包括空格")
    //
    //    val lengths = logs.map(line => line.size)
    //    val maxLen = lengths.reduce((a, b) => if (a > b) a else b)
    //    println(maxLen)
    //
    //    line("字数最多的行，不包括空格")
    //
    //    val wordCounts = logs.map(line => line.split("\\s+").size)
    //    val maxWords = wordCounts.reduce((a, b) => if (a > b) a else b)
    //    println(maxWords)
    //
    //    line("save error logs")

    //    注意 saveAsTextFile 里的参数是保存文件的路径，而不是实际的文件名
    //    errorLogs.saveAsTextFile("src/main/scala/data/error_logs")


    //    ==================================================

    val batchInterval = 10
    val ssc = new StreamingContext(sc, Seconds(batchInterval))

    ssc.start()

    ssc.awaitTermination()

  }

  def line(title: String = ""): Unit = {
    if (title == "") {
      println("==================")
    } else {
      println("========= " + title + " =========")
    }
  }
}
