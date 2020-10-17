import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


object SparkSQLAdvanced {
  def main(args: Array[String]): Unit = {
    //    不输出多余的info
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._

    //    --------------------------

    line()
    val badMovies = Seq(
      Row(null, null, null),
      Row(null, null, 2018L),
      Row("John Doe", "Awesome Movie", null),
      Row(null, "Awesome Movie", 2018L),
      Row("mary Jane", null, 2018L)
    )

    val movieSchema = StructType(Array(
      StructField("actor_name", StringType, true),
      StructField("movie_title", StringType, true),
      StructField("produced_year", LongType, true)
    ))

    val badMoviesRDD = spark.sparkContext.parallelize(badMovies)
    val badMoviesDF = spark.createDataFrame(badMoviesRDD, movieSchema)
    badMoviesDF.show()
    //    返回不是 null 的列的统计信息
    badMoviesDF.selectExpr("count(actor_name)", "count(movie_title)", "count(produced_year)", "count(*)").show()

    // ---------------

    doubleLine("Aggregations 聚集，聚合")

    line("count()")

    //    create a dataframe by reading dataset
    val flight_summaryDF = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/main/scala/data/flights/flight-summary.csv")
    println(flight_summaryDF.count())

    line("schema")

    flight_summaryDF.printSchema()

    line("count(col)")

    //    flight_summary.select("origin_airport").count()
    flight_summaryDF.select(count("origin_airport"), count("dest_airport") as "dest_count").show()


    line("countDistinct(col)")

    /**
     * 只统计不重复的列
     */

    flight_summaryDF.select(countDistinct("origin_airport"), countDistinct("dest_airport"), count("*")).show()

    line("aporox_count_distinct(col, max_estimated_error=0.05)")

    /**
     * 数据量比较大的时候，如果进行统计则会比较耗时。所以这里提供了一个【仅此、唯一、统计】的功能。approx, distinct, count
     * 设定的max_estimated_error值越大，运行的时间越短
     */
    flight_summaryDF.select(count("count"), countDistinct("count"), approx_count_distinct("count", 0.05)).show()

    line("min(col), max(col)")

    flight_summaryDF.select(min("count"), max("count")).show()

    line("sum(col)")

    flight_summaryDF.select(sum("count")).show()

    line("sumDistinct(col)")

    flight_summaryDF.select(sumDistinct("count")).show()

    line("avg(col)")

    flight_summaryDF.select(avg("count"), (sum("count") / count("count"))).show()

    line("skewness(col), kurtosis(col)")

    /**
     * 偏斜度    峰值
     */
    flight_summaryDF.select(skewness("count"), kurtosis("count")).show()

    line("variance(col), stddev(col)")

    /**
     * 方差   标准差
     * 这两个统计方式用来分析数据的分布情况。
     * 如果 variance 的值较小，意味着数据更接近平均值
     */

    flight_summaryDF.select(variance("count"), var_pop("count"), stddev("count"), stddev_pop("count")).show()

    doubleLine("Aggregation with Grouping")

    flight_summaryDF.groupBy("origin_airport").count().show(5, false) // truncate 截断

    flight_summaryDF.groupBy("origin_state", "origin_city").count().where('origin_state === "CA").orderBy('count.desc).show()

    doubleLine("Multiple Aggregations per Group")

    flight_summaryDF.groupBy("origin_airport").agg(
      count("count").as("count"),
      min("count").as("最小值"),
      max("count").as("最大值"),
      sum("count").as("合计")
    ).show()

    //    这是上面的语法的简写形式，但是不方便给统计出的列重命名
    flight_summaryDF.groupBy("origin_airport").agg(
      "count" -> "count",
      "count" -> "min",
      "count" -> "max",
      "count" -> "sum"
    ).show()

    doubleLine("Collection Group Values")

    /**
     * collect_list(col) 会返回可能带有重复值的统计信息
     * collect_set(col) 会返回唯一值的统计信息
     */
    val highCountDestCities = flight_summaryDF.where('count > 5500)
      .groupBy("origin_state")
      .agg(collect_list("dest_city").as("dest_cities"))
    highCountDestCities.withColumn("dest_city_count", size('dest_cities)).show(5, false)

    doubleLine("Aggregation with Pivoting")

    /**
     * 旋转是一种汇总数据的方法，方法是指定一个分类列，然后对另一个列执行聚合，这样分类值就从行转到单独的列中
     */
    val studentsDF = Seq(
      Student("John", "M", 180, 2015),
      Student("Mary", "F", 110, 2015),
      Student("Derek", "M", 200, 2015),
      Student("Julie", "F", 109, 2015),
      Student("Allison", "F", 105, 2015),
      Student("kirby", "F", 115, 2016),
      Student("Jeff", "M", 195, 2016)
    ).toDF()
    //    按照graduation_year这一列进行分组，然后基于gender性别，统计weight的平均值
    studentsDF.groupBy("graduation_year").pivot("gender").avg("weight").show()

    //    一次性可以做多项统计计算
    studentsDF.groupBy("graduation_year").pivot("gender").agg(
      min("weight").as("min"),
      max("weight").as("max"),
      avg("weight").as("avg")
    ).show()

    //    给出更精细的计算条件。这里提供了两个，分别为性别gender以及性别中的 M。这样更加细粒化的条件，能够加速程序的运行
    studentsDF.groupBy("graduation_year").pivot("gender", Seq("M")).agg(
      min("weight").as("min"),
      max("weight").as("max"),
      avg("weight").as("avg")
    ).show()

    doubleLine("Joins")

    val employeeDF = Seq(
      Employee("John", 31),
      Employee("Jeff", 33),
      Employee("Mary", 33),
      Employee("Mandy", 34),
      Employee("Julie", 34),
      Employee("Kurt", null.
        asInstanceOf[Int])
    ).toDF()

    val deptDF = Seq(
      Dept(31, "Sales"),
      Dept(33, "Engineering"),
      Dept(34, "Finance"),
      Dept(35, "Marketing")
    ).toDF()

    //    register them as views so we can use SQL for perform joins
    //    做这个操作的目的是，在后面使用SQL语言进行操作的时候，可以用新名字代替这里的DataFrame
    employeeDF.createOrReplaceTempView("employees")
    deptDF.createOrReplaceTempView("departments")

    line("Inner Joins")

    /**
     * 列的值在两个表中相同时，才会取出
     */

    //      定义一个等值比较的表达式
    val deptJoinExpression = employeeDF.col("dept_no") === deptDF.col("id")

    //    执行join操作
    employeeDF.join(deptDF, deptJoinExpression, "inner").show()

    //    inner join 是默认的join形式，所以不用特意的指定joinType。结果和上面的结果一样
    employeeDF.join(deptDF, deptJoinExpression).show()

    //    Using SQL
    spark.sql("select * from employees JOIN departments on dept_no==id").show()

    //    a shorter version of the join expression
    employeeDF.join(deptDF, 'dept_no === 'id).show()

    //    specify the join expression inside the join transformation
    employeeDF.join(deptDF, employeeDF.col("dept_no") === deptDF.col("id")).show()

    //    specify the join expression using the where transformation
    employeeDF.join(deptDF).where('dept_no === 'id).show()

    line("Left Outer Joins")

    //    the join type can be either "left_outer" or "leftouter"
    employeeDF.join(deptDF, 'dept_no === 'id, "left_outer").show()

    //    using SQL
    spark.sql("select * from employees LEFT OUTER JOIN departments on dept_no == id").show()

    line("Right Outer Joins")

    employeeDF.join(deptDF, 'dept_no === 'id, "rightouter").show()

    //    using SQL
    spark.sql("select * from employees RIGHT OUTER JOIN departments on dept_no == id").show()

    line("Outer Joins(aka 又叫做 Full Other Joins)")

    employeeDF.join(deptDF, 'dept_no === 'id, "outer").show()

    //    using SQL
    spark.sql("select * from employees FULL OUTER JOIN departments on dept_no == id").show()

    line("Left Anti-Joins")

    /**
     * 这种交叉的方式，是join的反操作。即找出左边表里的一些列与右边表对应列的值不相同的行
     */
    employeeDF.join(deptDF, 'dept_no === 'id, "left_anti").show()

    //    using SQL
    spark.sql("select * from employees LEFT ANTI JOIN departments on dept_no == id").show()

    line("Left Semi-Joins")

    /**
     * 这个操作是上面的 Left anti-join 的反操作。
     * 这个操作很像 inner join，只是这种交叉出来的数据集不包含右边的数据集的列，只包含左边表的列
     */

    employeeDF.join(deptDF, 'dept_no === 'id, "left_semi").show()

    //    using SQL
    spark.sql("select * from employees LEFT SEMI JOIN departments on dept_no == id").show()

    line("Cross (aka Cartesian)")

    /**
     * 这个操作不需要join表达式。
     * 这个操作比较危险，因为它会用左边表的每一行与右边表的每一行进行聚合。最终的结果是左边表的行数与右边表的行数的乘积。
     */

    val employee_X_DepartRows = employeeDF.crossJoin(deptDF).count()
    println(employee_X_DepartRows)

    //    using SQL and display up to 30 rows
    spark.sql("select * from employees CROSS JOIN departments").show(30)

    doubleLine("Deal with Duplicate Column Names")

    //    add a new column to deptDF with name dept_no
    //    给 deptDF添加一列，名为 dept_no，值与 id 这一列相同
    val deptDF2 = deptDF.withColumn("dept_no", 'id)
    deptDF2.printSchema()

    val dupNameDF = employeeDF.join(deptDF2, employeeDF.col("dept_no") === deptDF2.col("dept_no"))
    dupNameDF.printSchema()

    //    dupNameDF 里面有两个相同名称的列 dept_no，
    //    当运行 dupNameDF.select("dept_no") 时就会报错

    doubleLine("Use the Original DataFrame")

    //    在合并的DataFrame里，用原始的 DataFrame deptDF2 来指定dept_no 列
    dupNameDF.select(deptDF2.col("dept_no")).show()

    line("Rename column Before joining")

    /**
     * 对DataFrame的某个列重命名
     */
    dupNameDF.withColumnRenamed("dept_no", "dept_no2").show()

    line("Using a joined column name")

    val noDupNameDF = employeeDF.join(deptDF2, "dept_no")
    noDupNameDF.printSchema()

    doubleLine("Overview of a Join Implementation")

    /**
     * 在spark中，有两种join的方式。一种是shuffle hash join，另一种是broadcast join
     * 当两个dataset的尺寸较大时，用 shuffle hash，否则使用 broadcast join
     * spark会自动识别，并采用对应的join方式。但是，也可以在join是明确的指定join的方式
     */

    employeeDF.join(broadcast(deptDF), employeeDF.col("dept_no") === deptDF("id")).explain()

    //    using SQL
    spark.sql("select /*+ MAPJOIN(departments) */ * from employees JOIN departments on dept_no == id").explain()

    //    ==============================================
  }

  def line(title: String = ""): Unit = {
    println(s"\n-------------${title}---------------------\n")
  }

  def doubleLine(title: String = ""): Unit = {
    println(s"\n===========${title}============\n")
  }

  case class Student(name: String, gender: String, weight: Long, graduation_year: Long)

  case class Employee(first_name: String, dept_no: Long)

  case class Dept(id: Long, name: String)

}
