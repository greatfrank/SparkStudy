package HelloWorld

import org.apache.spark.{SparkConf, SparkContext}

object HelloWorld {

  case class Contact(id: Long, name: String, email: String)

  def toUpperCase(line: String): String = {
    line.toUpperCase
  }

  def main(args: Array[String]): Unit = {

    // 创建一个spark的配置类
    val conf = new SparkConf()
    // 设置在本地运行spark集群
    conf.setMaster("local")
    // 设置App的名称
    conf.setAppName("First Spark Application")
    // 创建一个spark的上下文。有了这个上下文context，后面就可以方便的调用API进行数据的处理
    val sparkContext = new SparkContext(conf)

    println("-----------------")

    //    将列表转换为一个RDD
    val rdd1 = sparkContext.makeRDD(Array(100, 200, 400, 250))
    //    调用RDD的collect方法，将分布的计算进行收集合并，最后遍历打印
    rdd1.collect().foreach(println)

    println("-----------------")

    //    创建一个sparkContext的列表，元素都是字符串类型
    val stringList = Array("Spark is awesome", "Spark is cool")
    //    sparkContext调用 parallelize方法，将列表进行并行化处理。这会返回一个RDD
    val stringRDD = sparkContext.parallelize(stringList)
    //    然后调用RDD的API，map的作用就是将数据处理的计算工作分配的集群中，但是注意这里只是本地单机的情况。因为不能修改原始的数据，所以这里的map方法会返回一个新的RDD。
    val allCapsRDD = stringRDD.map(line => line.toUpperCase)
    //    然后用这个新的RDD调用collect函数，将分布的计算统计在一起，最后遍历这个RDD，把每一个元素打印出来。
    allCapsRDD.collect().foreach(println)

    println("-----------------")

    //    还可以将RDD里的每一行都用一个自定义函数（toUpperCase）进行处理
    stringRDD.map(line => toUpperCase(line)).collect().foreach(println)

    println("======= map(func) ==========")

    //    定义一个sparkContextala列表
    val contactData = Array(
      "1#John Doe#jdoe@domain.com",
      "2#Mary Jane#jane@domain.com"
    )
    //    通过SparkContext的实例sparkContext，调用parallelize方法进行并行化处理，返回一个新的RDD
    val contactDataRDD = sparkContext.parallelize(contactData)
    //    RDD的map方法可以进行多重处理，把所有的处理逻辑包裹在一个花括号里
    val contactRDD = contactDataRDD.map(line => {
      //      将每一个元素用#拆分为列表
      val contactArray = line.split("#")
      //      然后将内容传递给一个case class，实例化出一个个的Contact的实例。这每一个Contact实例都是RDD的一个元素
      Contact(contactArray(0).toLong, contactArray(1), contactArray(2))
    })
    //    最后收集，并遍历打印
    contactRDD.collect().foreach(println)

    println("----------------")

    //    transform from a collection of Strings to a collection of Integers
    val stringLenRDD = stringRDD.map(line => line.length)
    stringLenRDD.collect().foreach(println)

    println("\n======= flatMap(func) ==========\n")

    //    transform lines to words
    val wordRDD = stringRDD.flatMap(line => line.split(" "))
    wordRDD.collect().foreach(println)

    /**
     * stringRDD.map(line => line.split(" ")).collect()
     * 得到：Array[Array[String]] = Array(Array(Spark, is, awesome), Array(Spark, is, cool))
     *
     * stringRDD.flatMap(line => line.split(" ")).collect()
     * 得到：Array[String] = Array(Spark, is, awesome, Spark, is, cool)
     *
     * 顾名思义，flatMap 会将每一行进行扁平化处理，最终只会得到一个数组。而 map 则会得到多个数组
     */

    println("\n======= filter(func) ==========\n")

    //    按照对应的规则对 RDD 进行过滤
    val awesomeLineRDD = stringRDD.filter(line => line.contains("awesome"))
    awesomeLineRDD.collect().foreach(println)

    println("\n======= mapPartitions(func)/mapPartitionsWithIndex(index, func) ==========\n")

    import scala.util.Random
    val simpleList = Array("One", "Two", "Three", "Four", "Five")

    def addRandomNumber(rows: Iterator[String]) = {
      val rand = new Random(System.currentTimeMillis() + Random.nextInt())
      rows.map(line => line + " : " + rand.nextInt())
    }

    //    扁平化处理，切分为 2 个切片
    val simpleRDD = sparkContext.parallelize(simpleList, 2)
    val result = simpleRDD.mapPartitions((rows: Iterator[String]) => addRandomNumber(rows))
    result.collect().foreach(println)

    println("-----------------")

    val numberRDD = sparkContext.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 2)
    //    ???? 并没有出现分片的序号 ????
    numberRDD.mapPartitionsWithIndex((idx: Int, itr: Iterator[Int]) => {
      itr.map(n => (idx, n))
    })
    numberRDD.collect().foreach(println)



    //    =======================================
  }


}
