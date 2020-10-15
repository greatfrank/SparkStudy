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
     * 提供的csv文件里，第一行是表头，叫做【header】。spark可以识别列的名称。
     */

    //  example 1 ---- 读取本地的csv文件
    println("--------- example 1 ----------")
    //    直接用csv文件的第一行表头所谓列的名称
    val myMovies = spark.read.option("header", "true").csv("src/main/scala/movies.csv")
    myMovies.printSchema()

    //   example 2 ---- infer（推断）schema 结构
    println("--------- example 2 ----------")
    //    如果提供inferSchema，程序会自动判断表头的名称的类型
    val myMovies2 = spark.read.option("header", "true").option("inferSchema", "true").csv("src/main/scala/movies.csv")
    myMovies.printSchema()

    //    手动提供（自定义）一个schema
    println("--------- example 3 ----------")
    import org.apache.spark.sql.types._
    //    定义这个值的目的是，不适用csv文件里默认的表头的列名称，而是自定义列名称
    val movieSchema = StructType(Array(
      StructField("actor_name", StringType, true),
      StructField("movie_title", StringType, true),
      StructField("produced_year", LongType, true)
    ))
    val myMovies3 = spark.read.option("header", "true").schema(movieSchema).csv("src/main/scala/movies.csv")
    myMovies3.printSchema()
    myMovies3.show(5)

    println("--------- Read a TSV file with the CSV Format --------")
    // tsv文件和csv文件类似，但是tsv是用制表符隔开每一列数据
    /**
     * 将上面的csv文件里定义的schema用在tsv文件的读取中
     */
    val myMovie4 = spark.read.option("header", "true").option("sep", "\t").schema(movieSchema).csv("src/main/scala/movies.tsv")
    myMovie4.printSchema()

    //    ======================================================
    //    ===== Create DataFrame by Reading JSON Files =========
    //    ======================================================
    println("==== Create DataFrame by Reading JSON Files ====")

    println("------ Auto detect column ----------")
    val myMoives5 = spark.read.json("src/main/scala/movies.json")
    //    spark 会自动识别列的名称和数据类型。这里会将第三列的年份识别为Long数据类型
    myMoives5.printSchema()

    println("------ Manually column name ---------")

    val movieSchema2 = StructType(Array(
      StructField("actor_name", StringType, true),
      StructField("movie_title", StringType, true),
      StructField("produced_year", IntegerType, true)
    ))

    val myMovies6 = spark.read.option("inferSchema", "true").schema(movieSchema2).json("src/main/scala/movies.json")
    myMovies6.printSchema()
    myMovies6.show(5)


    //    ======================================================
    //    ===== Create DataFrame by Reading Parquet Files =========
    //    ======================================================
    println("------ read parquet file by spark ------")

    val myMovie7 = spark.read.load("src/main/scala/movies.parquet")
    myMovie7.printSchema()
    val myMovie8 = spark.read.parquet("src/main/scala/movies.parquet")
    myMovie8.printSchema()
    myMovie8.show(5)

    //    ======================================================
    //    ===== Create DataFrame by Reading ORC Files =========
    //    ======================================================
    println("---- read ORC file by Spark ----")
    val myMovie9 = spark.read.orc("src/main/scala/movies.orc")
    myMovie9.printSchema()
    myMovie9.show(5)

    //    ======================================================
    //    ===== Work with Structured Operations =========
    //    ======================================================
    println("---- referring to a column ----")

    import org.apache.spark.sql.functions._
    val kvDF2 = Seq(
      (1, 2),
      (2, 3)
    ).toDF("key", "value")

    //    以下的效果都是相同的，很多都是语法糖。都是从DataFrame里选择key
    kvDF2.select("key").show()
    kvDF2.select(col("key")).show()
    kvDF2.select(column("key")).show()
    kvDF2.select($"key").show()
    kvDF2.select('key).show()

    println("-----")

    kvDF2.select(kvDF2.col("key")).show()
    kvDF2.select('key, 'key > 1).show()

    println("======= Work with structured transformations ====")

    println("======= select(columns) ============")
    val allMovies = spark.read.parquet("src/main/scala/movies.parquet")
    allMovies.select("movie_title", "produced_year").show(5)

    //    这里跟10相除后取余数，就是为了计算影片的年代
    allMovies.select('movie_title, ('produced_year - ('produced_year % 10)).as("produced_decade")).show(5)


    println("======= selectExpr(expressions) ============")

    // selectExpr 不仅会显示全部的列，还会根据算法添加新的计算列
    allMovies.selectExpr("*", "(produced_year - (produced_year % 10)) as decade").show(5)

    // 用计算列来展示，而不用原始的列
    allMovies.selectExpr("count(distinct(movie_title)) as movies", "count(distinct(actor_name)) as actors").show()

    println("======= filler(condition), where(condition) ============")

    allMovies.filter('produced_year < 2000).show()
    allMovies.where('produced_year > 2000).show()
    allMovies.filter('produced_year >= 2000).show()
    allMovies.where('produced_year >= 2000).show()
    allMovies.filter('produced_year === 2000).show(5)
    allMovies.select("movie_title", "produced_year").filter('produced_year =!= 2000).show(5)
    allMovies.filter('produced_year >= 2000 && length('movie_title) < 5).show(5)
    allMovies.filter('produced_year >= 2000).filter(length('movie_title) < 5).show(5)

    println("======= distinct, dropDuplicates ============")

    /**
     * 这两个函数具有相同的功能。都是去除相同的row。
     * dropDuplicates允许你控制在去除相同项的逻辑中使用哪一个列
     */
    allMovies.select("movie_title").distinct().selectExpr("count(movie_title) as movies").show()
    allMovies.dropDuplicates("movie_title").selectExpr("count(movie_title) as movies").show()

    println("======= sort(columns), orderBy(columns) ============")

    val movieTitles = allMovies.dropDuplicates("movie_title").selectExpr("movie_title", "length(movie_title) as title_length", "produced_year")
    movieTitles.sort('title_length).show(5)
    //    title_length采用倒序排列 (ascending / descending)
    movieTitles.orderBy('title_length.desc).show(5)
    movieTitles.orderBy('title_length.desc, 'produced_year).show(5)

    println("======= limit(n) ============")

    /**
     * 以DataFrame的格式返回前 n 行
     */
    val actorNameDF = allMovies.select("actor_name").distinct().selectExpr("*", "length(actor_name) as length")
    actorNameDF.orderBy('length.desc).limit(10).show()

    println("======= union(otherDataFrame) ============")

    /**
     * 拼接两个DataFrame。每一个DataFrame的列名称必须匹配，即具有相同的schema
     */
    //      add a missing actor to movie with title as "12"
    val shortNameMovieDF = allMovies.where('movie_title === "12")
    shortNameMovieDF.show()

    //    create a DataFrame with one row
    import org.apache.spark.sql.Row
    val forgottenActor = Seq(Row("Brychta, Edita", "12", 2007L))
    val forgottenActorRDD = spark.sparkContext.parallelize(forgottenActor)
    val forgottenActorDF = spark.createDataFrame(forgottenActorRDD, shortNameMovieDF.schema)
    val completeShortNameMovieDF = shortNameMovieDF.union(forgottenActorDF)
    completeShortNameMovieDF.show()

    println("======= withColumn(colName, column) ============")

    /**
     * 给现有的DataFrame添加新的列。
     * 用selectExpr同样能完成这样的工作。如果给到的列名称和现有的DataFrame的列同名，那么现有的列将会被给到的列替换。
     */
    //    这会添加一个新的列 decade
    allMovies.withColumn("decade", ('produced_year - 'produced_year % 10)).show(5)
    //    因为与原始的DataFrame的 produced_year 同名，所以会替换
    allMovies.withColumn("produced_year", ('produced_year - 'produced_year % 10)).show(5)

    println("======= withColumnRenamed(existingColName, newColName) ============")

    /**
     * 给DataFrame里现有的某个列重命名
     */
    allMovies.withColumnRenamed("actor_name", "actor")
      .withColumnRenamed("movie_title", "title")
      .withColumnRenamed("produced_year", "year")
      .show(5)

    println("======= drop(colName1, colName2) ============")

    /**
     * 删掉DataFrame里的某些列
     */
    allMovies.printSchema()
    //    me 这一列不存在，所以忽略，不报错
    allMovies.drop("actor_name", "me").printSchema()

    println("======= simple(fraction), sample(fraction, seed), sample(fraction, seed, withReplacement) ============")

    /**
     * 这个transformation返回从DataFrame里随机取得的一部分rows。
     * 取得的row的个数由fraction这个参数控制，它接收一个0~1之间的小数。
     * seed用于生成随机数
     */
    //    不重复的，取出0.03%的行
    allMovies.sample(false, 0.0003).show(5)
    //    可以重复取出row，取出0.03%的行，
    allMovies.sample(true, 0.0003, 123456).show(5)

    println("======= randomSplit(weights) ============")

    /**
     * 这个transformation的应用场景主要在机器学习。因为它可以将数据拆分为若干个部分，一部分是训练集，另一部分是测试集。
     * 这个方法会返回一个或多个DataFrame。返回的DataFrame集的个数由weights来确定。
     */
    //      weights必须由一个列表定义，表示将原始数据分为3部分，拆分的比例是 60% 30% 10%
    val smallerMovieDFs = allMovies.randomSplit(Array(0.6, 0.3, 0.1))
    println(allMovies.count())
    println(smallerMovieDFs(0).count())
    println(smallerMovieDFs(0).count() + smallerMovieDFs(1).count() + smallerMovieDFs(2).count())

    //    =====================================================
    //    =========  Work with Missing or Bad Data  ===========
    //    =====================================================

    println("---- Drop rows with missing data ----")

    import org.apache.spark.sql.Row

    val badMovies = Seq(
      Row(null, null, null),
      Row(null, null, 2018L),
      Row("John Doe", "Awesome Movie", null),
      Row(null, "Awesome Movie", 2018L),
      Row("mary Jane", null, 2018L)
    )
    val badMoviesRDD = spark.sparkContext.parallelize(badMovies)
    val badMoviesDF = spark.createDataFrame(badMoviesRDD, movieSchema)
    badMoviesDF.show()
    //drop rows that have missing data in any column
    // both of the lines below will achieve the same purpose
    badMoviesDF.na.drop().show()
    //    任何一列有null，这一行都会被去掉
    badMoviesDF.na.drop("any").show()

    //    每一列都是null时，这一行才会被去掉
    badMoviesDF.na.drop("all").show()

    //    actor_name 这一列的值是null的时候，这一行才会被去掉
    badMoviesDF.na.drop(Array("actor_name")).show()


    //    =====================================================
    //    =========  Work with Datasets  ===========
    //    =====================================================

    println("====== Create Datasets ======")

    val localMoviesDF = Seq(
      Movie("John Doe", "超级查派", 2018L),
      Movie("Mary Jane", "未来战士", 2019L),
      Movie("Mary Jane", "1917", 2018L),
      Movie("Mary Jane", "Awesome movie", 2020L),
      Movie("Tom Hanks", "Saving private Ryan", 1998L),
      Movie("马克·达蒙", "谍影重重", 2004L),
    )

    val localMoviesDataset1 = spark.createDataset(localMoviesDF)
    val localMoviesDataset2 = localMoviesDF.toDS()
    localMoviesDataset1.show()
    localMoviesDataset2.show()

    println("---- Working with Dataset ----")

    localMoviesDataset1.filter(movie => movie.produced_year == 2018).show()

    println(localMoviesDataset1.first().movie_title)

    val titleWithYearsDS = localMoviesDataset1.map(m => (m.movie_title, m.produced_year))
    titleWithYearsDS.printSchema()

    //    =======================================================
    //    ============== Running SQL in Spark ==========================
    //    =======================================================

    println("==== Running SQL in Spark ====")

    //    display tables in the catalog
    spark.catalog.listTables().show()

    //    register moveis DataFrame as a temporary view
    //    这里给movieDF这个DataFrame起了一个别名，叫做 movies
    moviesDF.createOrReplaceTempView("movies")

    //    再在目录中展示表格
    spark.catalog.listTables().show()

    //    show the list of columns of movies view in catalog
    spark.catalog.listColumns("movies").show()

    //    register movies as global temporary view called movies_g
    moviesDF.createOrReplaceGlobalTempView("movies_g")

    val infoDF = spark.sql("select current_date() as today, 1 + 100 as value")
    infoDF.show()

    //    直接从本地的parquet文件里读取数据，运行SQL语句
    spark.sql("SELECT * FROM parquet.`src/main/scala/movies.parquet`").show()

    spark.sql("SELECT * FROM parquet.`src/main/scala/movies.parquet` where produced_year > 2009").show()

    spark.sql("select actor_name, count(*) as count from parquet.`src/main/scala/movies.parquet` group by actor_name")
      .where('count > 30)
      .orderBy('count.desc)
      .show()

    spark.sql(
      """select produced_year, count(*) as count
        |from (select distinct movie_title, produced_year from
        |parquet.`src/main/scala/movies.parquet`)
        |group by produced_year
        |""".stripMargin)
      .orderBy('count.desc)
      .show()

    spark.sql("select count(*) as total_movies from parquet.`src/main/scala/movies.parquet`").show()

    //    =======================================================
    //    ========= Write Data out to Storage Systems ===========
    //    =======================================================

    println("======== Write Data out to Storage Systems =========")

    //    使用datafram的write函数，将DataFrame的数据写入到文件中
    /**
     * 将数据以csv的格式输出到文件，用#作为分隔符。下面的这个路径仅仅表示目录，最终写入的文件会有很多，文件名是spark自动定义的。
     */
    //    allMovies.write.format("csv").option("sep", "#").save("src/main/scala/output/csv")

    /**
     * Mode有多种。
     * append：追加写入
     * overwrite：覆盖写入
     * error/errorIfExists/default：这是默认的模式，如果指定的路径存在，那么将会抛出错误
     * ignore：如果指定的路径存在，写入的动作则会停止
     */
    allMovies.write.format("csv").mode("overwrite").option("sep", "#").save("src/main/scala/output/csv")

    println(allMovies.rdd.getNumPartitions)

    //    reduce the number of partitions in a DataFrame to 1
    val singlepartitionDF = allMovies.coalesce(1)

    allMovies.write.partitionBy("produced_year").mode("overwrite").save("src/main/scala/output/produced_year")


    //    ====================================================
  }

  case class Movie(actor_name: String, movie_title: String, produced_year: Long)

}
