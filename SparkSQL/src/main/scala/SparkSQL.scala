import org.apache.spark.sql.SparkSession
import scala.util.Random

object SparkSQL {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[4]").getOrCreate()
    import spark.implicits._

    //    --------------------
    //    本书资源下载地址： https://github.com/Apress/beginning-apache-spark-2

    val rdd = spark.sparkContext.parallelize(1 to 10).map(x => (x, Random.nextInt(100) * x))

    /**
     * ======================
     * 创建DataFrame的方式一
     * convert RDD to schema
     * =====================
     */
    println("创建DataFrame的方式一")
    //    经过测试，这里的key和value用中文字符串也可以
    val kvDF = rdd.toDF("key", "value")

    /**
     * print the Schema（架构） and show the Data of a DataFrame
     */
    kvDF.printSchema()
    //    显式所有数据
    kvDF.show()
    //    显式前 5 行数据
    kvDF.show(5)

    /**
     * ======================
     * 创建DataFrame的方式二
     * programmatically create a schema
     * =====================
     */
    println("创建DataFrame的方式二")
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types._

    val peopleRDD = spark.sparkContext.parallelize(
      Array(
        //        这里的L表示Long数据类型，即长整型
        Row(1L, "John Doe", 30),
        Row(2L, "Mary Jane", 25)
      )
    )
    val schema = StructType(Array(
      //      name, dataType, nullable
      StructField("id", LongType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true)
    ))
    val peopleDF = spark.createDataFrame(peopleRDD, schema)
    peopleDF.printSchema()
    peopleDF.show()

    //    ===== DataFrame 与 scala 数据类型的对应关系 ========
    /**
     * Data Type <------> Scala Type
     * BooleanType <------> Boolean
     * ByteType <------> Byte
     * ShortType <------> Short
     * IntegerType <------> Int
     * LongType <------> Long
     * FloatType <------> Float
     * DoubleType <------> Double
     * DecimalType <------> java.math.BigDecial
     * StringType <------> String
     * BinaryType <------> Array[Byte]
     * TimestampType <------> java.sql.Timestamp
     * DateType <------> java.sql.Date
     * ArrayType <------> scala.collection.Seq
     * MapType <------> scala.collection.Map
     * StructType <------> org.apache.spark.sql.Row
     */

    /**
     * ======================
     * 创建DataFrame的方式三
     * 利用类SparkSession，调用它的range方法，可以方便的从一个单独的LongType类型的id的列来创建DataFrame。
     * 但是要注意，range方法只能创建一个单独的DataFrame列
     * =====================
     */
    println("创建DataFrame的方式三")
    val df1 = spark.range(5).toDF("num").show()
    spark.range(5, 10).toDF("num").show()
    spark.range(5, 15, 2).toDF("序号").show()

    println("====== 从tuple创建多个DataFrame列 =========")

    val movies = Seq(
      ("Damon, Matt", "The Bourne Ultimatum", 2007L),
      ("Damon, Matt", "Good Will Hunting", 1997L)
    )
    val moviesDF = movies.toDF("actor", "title", "year")
    moviesDF.printSchema()
    moviesDF.show()

    /**
     * Create DataFrame from Data Sources
     * 从其他数据源获取数据
     * 格式如下： spark.read.format(...).option("key","value").schema(...).load()
     */

    /**
     * | Name | Optional | Comments
     * | format | No | This can be one of the built-in data sources or a custom format. For a built-in format, you can use a short name (json, parquet, jdbc, orc, csv, text). For a custom data source, you need to provide a fully qualified name. See Listing 4-10 for details and examples.
     *
     * | option | Yes | DataFrameReader has a set of default options for each data source format. You can override those default values by providing a value to the option function.
     *
     * | schema | Yes | Some data sources have the schema embedded inside the data files, i.e., Parquet and ORC. In those cases, the schema is automatically inferred. For other cases, you may need to provide a schema.
     */

    /**
     * spark.read.json("<path>")
     * spark.read.format("json")
     * spark.read.parquet("<path>")
     * spark.read.format("parquet")
     * spark.read.jdbc
     * spark.read.format("jdbc")
     * spark.read.orc("<path>")
     * spark.read.format("orc")
     * spark.read.csv("<path>")
     * spark.read.format("csv")
     * spark.read.text("<path>")
     * spark.read.format("text")
     *
     * // custom data source -- fully qualifed package name
     * spark.read.format("org.example.mysource")
     */

    println("======== Create DataFrames by Reading Text Files =======")

    /**
     * text files 含有非结构化的数据，那么在文件里的每一行就变成了DataFrame的一个row。
     * 针对这种文本文件的内容，典型的处理方式就是一空格作为定位符，将内容进行拆分。然后查看里面是否有一些特别的单词。
     */

    val textFile = spark.read.text("src/main/scala/README.md")
    textFile.printSchema()
    //    读取前5行，而且不截断
    textFile.show(5, false)
    println("~.~.~.~.~.~.~.~.~.~.~.~.~.~.")
    textFile.show() // 默认只读取前20行

    println("======== Create DataFrames by Reading CSV Files =======")

    /**
     * CSV 文件类似表格。默认的，它的每一行的末尾都有一个逗号作为一行的分隔符。有些CSV文件有表头，有些则没有。
     */


    //    ====================================================
  }
}
