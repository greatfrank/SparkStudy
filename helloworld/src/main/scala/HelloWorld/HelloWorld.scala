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

    println("\n======= union(otherRDD) ==========\n")
    //    合并两个RDD。将一个RDD插入到另外一个RDD中，最终生成一个新的RDD
    val rdd_1 = sparkContext.parallelize(Array(1, 2, 3, 4, 5))
    val rdd_2 = sparkContext.parallelize(Array(1, 6, 7, 8))
    val rdd_3 = rdd_1.union(rdd_2)
    rdd_3.collect().foreach(println)

    println("\n======= intersection(otherRDD) ==========\n")
    /**
     * 检查哪些row存在于两个或两个以上的RDD中，把共同存在于两个RDD中的那个row取出来，返回一个新的RDD
     * 类似于【并集】
     */
    val rdd_11 = sparkContext.parallelize(Array("One", "Two", "Three"))
    val rdd_22 = sparkContext.parallelize(Array("two", "One", "threed", "One"))
    val rdd_33 = rdd_11.intersection(rdd_22)
    rdd_33.collect().foreach(println)

    println("\n======= subtract(otherRDD) ==========\n")
    /**
     * subtract 类似寻找两个RDD中的row之间的差集。
     */
    val words = sparkContext.parallelize(List("The amazing thing about spark is that it is very simple to learn")).flatMap(l => l.split(" ")).map(w => w.toLowerCase)
    val stopWords = sparkContext.parallelize(List("a that this the it is to that")).flatMap(line => line.split(" "))
    val realWords = words.subtract(stopWords)
    realWords.collect().foreach(println)

    println("\n======= distinct() ==========\n")
    /**
     * remove any duplicate rows、它会计算每一个row的哈希码，然后比较这些哈希码，把相同哈希码的row去掉，只留下内容唯一的row
     */
    val duplicateValueRDD = sparkContext.parallelize(List("one", 1, "two", 2, "three", "one", "two", 1, 2))
    duplicateValueRDD.distinct().collect().foreach(println)

    println("\n======= sample(withReplacement, fraction, seed) ==========\n")
    /**
     * sample用于从全集中抽取一部分。
     * withReplacement：表示抽出样本后是否放回去。类型为Boolean。如果值为true，则抽取后还会放回去。那么这样意味着抽出的样本可能会有重复
     * fraction：double类型，值在0-1之间，0.3表示抽出30%
     * seed：表示一个种子，根据这个seed随机抽取，一般不设定，用默认的就可以。这个参数一般用于调试，可以将这个参数设定为定值
     */
    val numbers = sparkContext.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 2)
    numbers.sample(true, 0.3).collect().foreach(println)
    println("----")
    numbers.sample(false, 0.4).collect().foreach(println)

    //    =================================================
    //    ========== Actions ==============================
    //    =================================================

    println("\n======= collect() ==========\n")
    /**
     * 从每一个部分集合所有的row，然后把汇集到的row提交给其他程序进行处理
     * 但是如果你的row非常多，比如上百万个，那么这种方式就可能会出现内存溢出的错误，因为数据一下子有太多的数据放入了内从中。
     */
    val numbersRDD = sparkContext.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 2)
    numbersRDD.collect().foreach(println)

    println("\n======= count() ==========\n")

    /**
     * 顾名思义，它会统计row的个数，返回一个整数
     */
    println(numbersRDD.count())

    println("\n======= first() ==========\n")

    /**
     * 得到RDD中的第一个row。这里的“第一”表示在第一个partition中的。
     * 注意：如果你的RDD是空的，那么运行这个方法会报错
     */
    println(numbersRDD.first())


    println("\n======= take(n) ==========\n")

    /**
     * 返回在第一个partition中的RDD里的前 n 个row
     * take(1) 等同于 first()
     */
    numbersRDD.take(6).foreach(println)


    println("\n======= reduce(func) ==========\n")

    /**
     * 给reduce里传递一个函数。这里以求和为例。作用就是将RDD里的所有row的值累加起来
     */
    def add(v1: Int, v2: Int): Int = {
      println(s"v1: $v1, v2: $v2 => (${v1 + v2})")
      v1 + v2
    }

    numbersRDD.reduce(add)

    println("\n======= takeSample(withReplacement, n, [seed]) ==========\n")

    /**
     * 这个方法的作用于sample transformation 的作用类似，不同的是这个方法会返回一个含有row的列表
     */

    println("\n======= takeOrdered(n, [ordering]) ==========\n")

    /**
     * 这个action会返回具有一定次序的 n 个 rows
     */

    numbersRDD.takeOrdered(4).foreach(println)
    println("----")
    numbersRDD.takeOrdered(4)(Ordering[Int].reverse).foreach(println)

    println("\n======= top(n, [ordering]) ==========\n")

    /**
     * 获取RDD中的前n个值最大的row
     */
    numbersRDD.top(5).foreach(println)

    println("\n======= saveAsTextFile(path) ==========\n")

    //    =====================================================
    //    ============== Key/Value Pair RDD ===================
    //    =====================================================

    println("\n======= creating key/value pair RDD ==========\n")

    /**
     * key/value 可以是简单的scala数据类型，也可以是复杂的值，比如object，collection of objects，或者是tuple。
     */

    val rdd = sparkContext.parallelize(List("Spark", "is", "an", "amazing", "piece", "of", "technology"))
    val pairRDD = rdd.map(w => (w.length, w))
    pairRDD.collect().foreach(println)

    println("\n======= groupByKey([numTasks]) ==========\n")
    /**
     * 按照key进行分组
     * 下面的例子中，w.length 就会作为每一个单词的key。如果某些单词的字母数相同，那么将会分为一组
     */
    val rdd2 = sparkContext.parallelize(List("Spark", "is", "an", "amazing", "piece", "of", "technology"))
    val pairRDD2 = rdd2.map(w => (w.length, w))
    val wordByLenRDD = pairRDD2.groupByKey()
    wordByLenRDD.collect().foreach(println)

    println("\n======= reduceByKey(func, [numTasks]) ==========\n")
    /**
     * reduceByKey接收一个函数，reduceByKey会根据这个函数，按照相同的key，把对应的value进行处理。
     */

    val candyTx = sparkContext.parallelize(List(
      ("candy1", 5.2),
      ("candy2", 3.5),
      ("candy1", 2.0),
      ("candy2", 6.0),
      ("candy3", 3.0)
    ))
    val summaryTx = candyTx.reduceByKey((total, value) => total + value)
    summaryTx.collect().foreach(println)

    println("\n======= sortByKey([ascending], [numTasks]) ==========\n")

    /**
     * 顾名思义，根据key进行排序
     */
    //      这里通过map重新构建了元祖列表，用元祖的第二个元素作为key，因为这个key是数字，所以方便排序。用元祖的第一个元素作为value。
    val summaryByPrice = summaryTx.map(t => (t._2, t._1)).sortByKey()
    summaryByPrice.collect().foreach(println)

    println("-----")

    //    默认是升序排序，如果传递一个false，则是倒序排序
    val summaryByPrice2 = summaryTx.map(t => (t._2, t._1)).sortByKey(false)
    summaryByPrice2.collect().foreach(println)

    println("\n======= join(otherRDD) ==========\n")

    /**
     * 按照相同的key，将两个RDD里对应的value进行组合
     */
    val memberTx = sparkContext.parallelize(List(
      (110, 50.35),
      (127, 305.2),
      (126, 211.0),
      (105, 6.0),
      (165, 31.0),
      (110, 40.11))
    )
    val memberInfo = sparkContext.parallelize(List(
      (110, "a"),
      (127, "b"),
      (126, "b"),
      (105, "a"),
      (165, "c"))
    )
    val memberTxInfo = memberTx.join(memberInfo)
    memberTxInfo.collect().foreach(println)

    //    ==================================================
    //    ============= key/value pair RDD Actions ==================
    //    ==================================================

    println("\n======= countByKey() ==========\n")

    /**
     * 这个方法返回的数据类型是scala的map类型
     */

    val candyTx2 = sparkContext.parallelize(List(
      ("candy1", 5.2),
      ("candy2", 3.5),
      ("candy1", 2.0),
      ("candy3", 6.0)
    ))
    println(candyTx2.countByKey())

    println("\n======= collectAsMap() ==========\n")

    val candyTx3 = sparkContext.parallelize(List(
      ("candy1", 5.2),
      ("candy2", 3.5),
      ("candy1", 2.0),
      ("candy3", 6.0)
    ))
    //    多个相同key的row将会被收缩为一个唯一的row
    println(candyTx3.collectAsMap())

    println("\n======= collectAsMap() ==========\n")

    /**
     * 用于检测某个key是否存在于RDD中
     */
    val candyTx4 = sparkContext.parallelize(List(
      ("candy1", 5.2),
      ("candy2", 3.5),
      ("candy1", 2.0),
      ("candy3", 6.0)
    ))
    println(candyTx4.lookup("candy1"))
    println(candyTx4.lookup("candy2"))
    println(candyTx4.lookup("candy3"))
    println(candyTx4.lookup("candy5"))














    //    =======================================
  }


}
