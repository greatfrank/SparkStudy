import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object Run {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    //    set configuration info
    val conf = new SparkConf().setMaster("local[*]").setAppName("My App")
    //    create a instance of SparkContext
    val sc = new SparkContext(conf)

    //    ============= 统计出文件里的单词【Spark】的个数 =================

    //    load local file and create a RDD
    val linesRDD = sc.textFile("src/main/scala/data/README.md")
    //    调用filter这个API，创建出一个新的RDD，名为 sparkLines。
    //    这里实在过滤了所有行之后，将包含有【Spark】的行保留，生成最后的sparkLines这个RDD。注意这里的过滤条件—— Spark单词 是区分大小写的
    val sparkLines = linesRDD.filter(line => line.contains("Spark"))
    //    打印RDD中的第一个元素
    println(sparkLines.first())
    //    统计RDD中元素的个数
    println(sparkLines.count())

    //  ============ 统计文件中单词的个数 ==============

    //    切分成一个个的单词，生成新的RDD
    val wordsRDD = linesRDD.flatMap(line => line.split(" "))
    // 转换为键值对并计数
    val counts = wordsRDD.map(word => (word, 1)).reduceByKey((x, y) => x + y)
    //    打印出文件中公有多少个单词
    println(counts.count())

    //============================
  }
}
