import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._


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

    doubleLine("Working with Build-in Functions")

    line("Date-TIme Functions")

    val testDateTSDF = Seq(
      (1, "2018-01-01", "2018-01-01 15:04:58:865", "01-01-2018", "12-05-2017 45:50")
    ).toDF("id", "date", "timestamp", "date_str", "ts_str")

    //    convert these strings into date, timestamp and unix timestamp and specify a custom date and timestamp format
    val testDateResultDF = testDateTSDF.select(
      to_date('date).as("date1"),
      to_timestamp('timestamp).as("ts1"),
      to_date('date_str, "MM-dd-yyyy").as("date2"),
      to_timestamp('ts_str, "MM-dd-yyyy mm:ss").as("ts2"),
      unix_timestamp('timestamp).as("unix_ts")
    )

    testDateResultDF.printSchema()
    testDateResultDF.show()

    line("Convert a Date, Timestamp, Unix Timestamp to a String")

    testDateResultDF.select(
      date_format('date1, "dd-MM-YYYY").as("date_str"),
      date_format('ts1, "dd-MM-YYYY HH:mm:ss").as("ts_str"),
      from_unixtime('unix_ts, "dd-MM-YYYY HH:mm:ss").as("unix_ts_str")
    ).show(false)

    line("Date-time Calculation 日期时间的计算")

    //    构建DataFrame
    val employeeData = Seq(
      ("John", "2016-01-01", "2017-10-15"),
      ("May", "2017-02-06", "2017-12-25")
    ).toDF("name", "join_date", "leave_date")

    employeeData.show()

    //    计算日期和月份
    line("计算日期和月份")

    employeeData.select(
      'name,
      datediff('leave_date, 'join_date).as("days 从入职到离职的天数"),
      months_between('leave_date, 'join_date).as("months 从入职到离职的月份"),
      last_day('leave_date).as("last_day_of_month 本月的最后一天的日期")
    ).show(false)

    //    运行日期的加法和减法
    val oneDate = Seq(("2018-01-01")).toDF("new_year")

    oneDate.select(
      //      日期加上14天
      date_add('new_year, 14).as("mid_month"),
      //      日期减去 1 天
      date_sub('new_year, 1).as("new_year_eve"),
      //      当前日期到下一个周一是几月几日
      next_day('new_year, "Mon").as("next_mon")
    ).show(false)

    val valentimeDateDF = Seq(("2018-02-14 05:35:55")).toDF("date")
    valentimeDateDF.select(
      year('date).as("year"),
      quarter('date).as("quarter 季度"),
      month('date).as("month"),
      weekofyear('date).as("woy 当年的第几周"),
      dayofmonth('date).as("dom 当月的第几天"),
      hour('date).as("hour"),
      minute('date).as("minute"),
      second('date).as("second")
    ).show(false)

    doubleLine("Work with String Functions")

    val sparkDF = Seq(("   Spark   ")).toDF("name")

    line("trim 去掉字符串边上的空格")

    //    trimming 修边
    sparkDF.select(
      //      去掉字符串左右两边的空格
      trim('name).as("trim"),
      //      去掉字符串左边的空格
      ltrim('name).as("ltrim"),
      //      去掉字符串右边的空格
      rtrim('name).as("rtrim")
    ).show(false)

    line("向左边或者右边插入指定个数的字符串")

    sparkDF.select(
      //      去掉左右的空格
      trim('name).as("trim")
    )
      .select(
        //        向左边插入8个【-】
        lpad('trim, 8, "-").as("lpad"),
        //        向右边插入8个【=】
        rpad('trim, 8, "=").as("rpad"),
      )
      .show()

    line("对字符串进行 大写，小写，拼接，倒序 的处理")

    val sparkAwesomeDF = Seq(("Spark", "is", "awesome")).toDF("subject", "verb", "adj")
    sparkAwesomeDF.select(concat_ws(" ", 'subject, 'verb, 'adj).as("sentence"))
      .select(
        lower('sentence).as("lower"),
        upper('sentence).as("upper"),
        //        句子中的每一个单词第一个字母大写
        initcap('sentence).as("initcap"),
        reverse('sentence).as("reverse")
      ).show()

    line("transform from one character to another")
    //    把单词【Spark】里的【ar】替换为【oc】
    sparkAwesomeDF.select('subject, translate('subject, "ar", "oc")).show()

    line("使用 regexp_extract 函数来提取 【fox】 利用正则表达式")

    val rhymeDF = Seq(("A fox saw a crow sitting on a tree singing \"Caw! Caw! Caw!\"")).toDF("rhyme")

    //    regexp_extract 利用正则表达式从字符串中提取一段单词
    rhymeDF.select(regexp_extract('rhyme, "[a-z]*o[xw]", 0).as("substring")).show()

    //    用正则表达式将字符串中的一段字母替换掉
    rhymeDF.select(regexp_replace('rhyme, "fox|crow", "animal").as("new_rhyme")).show(false)
    //    和上面是同样的结果
    rhymeDF.select(regexp_replace('rhyme, "[a-z]*o[xw]", "animal").as("new_rhyme")).show(false)

    doubleLine("Work with Math Function 四舍五入")

    val numberDF = Seq((3.14)).toDF("pie")
    numberDF.select(
      round('pie).as("原始数据"),
      round('pie, 1).as("保留一位小数"),
      round('pie, 2).as("保留两位小数")
    ).show(false)

    doubleLine("Work with Collection Functions")

    val tasksDF = Seq(
      ("Monday", Array("Pick Up John", "Buy Milk", "Pay Bill"))
    ).toDF("day", "tasks")

    tasksDF.printSchema()

    //    获取列表的元素个数，排序，查看某些单词是否存在于列表中
    tasksDF.select(
      'day,
      size('tasks).as("size"),
      sort_array('tasks).as("sorted_tasks"),
      array_contains('tasks, "Pay Bill").as("shouldPayBill")
    ).show(false)

    //    explode 函数将会针对列表中的每一个元素创建一个新的行
    tasksDF.select('day, explode('tasks)).show(false)

    line("create a string that contains JSON string")

    //    经过测试，把json字符串都放在一行，正常运行；如果把这些字符串排列在多行，下面的转换会得到null的结果
    val todos = """{"day":"Monday","tasks":["Pick Up John","Buy Milk","Pay Bill"]}"""
    //        用上面的json形式的字符串生成一个DataFrame
    val todoStrDF = Seq((todos)).toDF("todos_str")

    //    todoStrDF is DataFrame with one column of string type
    todoStrDF.printSchema()

    line("Convert a JSON string into a Spark struct data type")

    //    这里定义一个json的结构，每一个key的数据类型是字符串，对应的value的数据类型由add方法的第二个参数定义
    val todoSchema = new StructType().add("day", StringType).add("tasks", ArrayType(StringType))

    //    使用 from_json 来转换 JSON 字符串。这里用的是别名 todos_str
    val todosDF = todoStrDF.select(from_json('todos_str, todoSchema).as("todos"))

    todosDF.printSchema()

    todosDF.select(
      'todos.getItem("day"),
      'todos.getItem("tasks"),
      'todos.getItem("tasks").getItem(0).as("first_task")
    ).show(false)

    line("Convert a Spark struct data type to JSON string")

    todosDF.select(to_json('todos)).show(false)

    doubleLine("混杂函数")

    line("单调递增")

    /**
     * range 函数会生成一个数据范围
     * 第一个参数：起始数字
     * 第二个参数：结束的数字
     * 第三个参数：数字的间隔
     * 第四个参数：分区，类似于分布式
     */
    val numDF = spark.range(1, 11, 1, 5)
    numDF.show(false)

    /**
     * +---+
     * |id |
     * +---+
     * |1  |
     * |2  |
     * |3  |
     * |4  |
     * |5  |
     * |6  |
     * |7  |
     * |8  |
     * |9  |
     * |10 |
     * +---+
     */

    //    查看数字范围的分区
    println(numDF.rdd.getNumPartitions)

    numDF.select(
      'id,
      //      单调递增
      monotonically_increasing_id().as("m_ii"),
      //      spark的分区编号
      spark_partition_id().as("partition")
    ).show(false)

    /**
     * +---+-----------+---------+
     * |id |m_ii       |partition|
     * +---+-----------+---------+
     * |1  |0          |0        |
     * |2  |1          |0        |
     * |3  |8589934592 |1        |
     * |4  |8589934593 |1        |
     * |5  |17179869184|2        |
     * |6  |17179869185|2        |
     * |7  |25769803776|3        |
     * |8  |25769803777|3        |
     * |9  |34359738368|4        |
     * |10 |34359738369|4        |
     * +---+-----------+---------+
     */

    doubleLine("用 when 函数将 数字类型的值转换为字符串")

    //    用7个数字代表一周的每一天
    val dayOfWeekDF = spark.range(1, 8, 1)

    //    将数字转换为字符串
    dayOfWeekDF.select(
      'id,
      when('id === 1, "Mon")
        .when('id === 2, "Tue")
        .when('id === 3, "Wed")
        .when('id === 4, "Thu")
        .when('id === 5, "Fri")
        .when('id === 6, "Sat")
        .when('id === 7, "Sun").as("dow")
    ).show(false)

    /**
     * +---+---+
     * |id |dow|
     * +---+---+
     * |1  |Mon|
     * |2  |Tue|
     * |3  |Wed|
     * |4  |Thu|
     * |5  |Fri|
     * |6  |Sat|
     * |7  |Sun|
     * +---+---+
     */


    dayOfWeekDF.select(
      'id,
      when('id === 6, "Weekend")
        .when('id === 7, "Weekend")
        .otherwise("Weekday").as("day_type")
    ).show(false)

    /**
     * +---+--------+
     * |id |day_type|
     * +---+--------+
     * |1  |Weekday |
     * |2  |Weekday |
     * |3  |Weekday |
     * |4  |Weekday |
     * |5  |Weekday |
     * |6  |Weekend |
     * |7  |Weekend |
     * +---+--------+
     */

    line("用 coalesce 函数处理列中的null值")

    val badMoviesDF2 = Seq(
      Movie(null, null, 2018L),
      Movie("John Doe", "Awesome Movie", 2018L)
    ).toDF()

    badMoviesDF2.show(false)

    //    检查actor_name这一列里是否有null，如果有，则用后面lit函数里的no_name字符串来替换null。
    //      coalesce函数，第一个参数是查询的列的名称，第二个参数lit函数里是要将null替换为什么字符串
    badMoviesDF2.select(coalesce('actor_name, lit("no_name")).as("new name")).show(false)

    doubleLine("用户自定义函数")

    val studentDF = Seq(
      Student2("Joe", 85),
      Student2("Jane", 90),
      Student2("Mary", 55)
    ).toDF()

    studentDF.show(false)

    /**
     * +----+-----+
     * |name|score|
     * +----+-----+
     * |Joe |85   |
     * |Jane|90   |
     * |Mary|55   |
     * +----+-----+
     */

    //    register as a view，注册一个表，为的是后面使用SQL
    studentDF.createOrReplaceTempView("students")

    //    用户自定义函数：把成绩的数字转换为档次
    def letterGrade(score: Int) = {
      score match {
        case score if score > 100 => "Cheating"
        case score if score >= 90 => "A"
        case score if score >= 80 => "B"
        case score if score >= 70 => "C"
        case _ => "F"
      }
    }

    //    把自定义函数注册为UDF  user defined function
    val letterGradeUDF = udf(letterGrade(_: Int): String)

    //    用 UDF 将成绩转换为字符串
    studentDF.select($"name", $"score", letterGradeUDF($"score").as("grade")).show(false)

    /**
     * +----+-----+-----+
     * |name|score|grade|
     * +----+-----+-----+
     * |Joe |85   |B    |
     * |Jane|90   |A    |
     * |Mary|55   |F    |
     * +----+-----+-----+
     */

    line("用SQL来调用UDF函数")

    spark.sqlContext.udf.register("letterGrade", letterGrade(_: Int): String)
    spark.sql("select name, score, letterGrade(score) as grade from students").show(false)

    /**
     * +----+-----+-----+
     * |name|score|grade|
     * +----+-----+-----+
     * |Joe |85   |B    |
     * |Jane|90   |A    |
     * |Mary|55   |F    |
     * +----+-----+-----+
     */

    //    ========================================
    //    =============== 高级分析函数 =============
    //    ========================================

    doubleLine("高级分析函数")

    line("rollup 聚合")

    /**
     * 可以根据部分列的数据进行归纳
     */

    //      通过where提供的条件，过滤出合格的row
    val twoStatesSummary = flight_summaryDF.select('origin_state, 'origin_city, 'count)
      .where('origin_state === "CA" || 'origin_state === "NY")
      .where('count > 1 && 'count < 20)
      .where('origin_city =!= "White Plains")
      .where('origin_city =!= "Newburgh")
      .where('origin_city =!= "Mammoth Lakes")
      .where('origin_city =!= "Ontario")

    twoStatesSummary.show(false)

    /**
     * +------------+-------------+-----+
     * |origin_state|origin_city  |count|
     * +------------+-------------+-----+
     * |NY          |New York     |4    |
     * |NY          |Elmira       |15   |
     * |CA          |San Diego    |18   |
     * |CA          |San Francisco|14   |
     * |NY          |New York     |2    |
     * |NY          |Albany       |5    |
     * |CA          |San Francisco|2    |
     * |NY          |Albany       |3    |
     * |NY          |Albany       |9    |
     * |NY          |New York     |4    |
     * |CA          |San Francisco|5    |
     * |CA          |San Diego    |4    |
     * |NY          |New York     |10   |
     * +------------+-------------+-----+
     */

    //    然后再利用rollup进行计算和归纳
    /**
     * 假设有个 3 列的数据要统计
     * rollup 会返回【column 1】、【column 1，column 2】,【column 1，column 2，column 3】的组合统计
     */
    twoStatesSummary.rollup('origin_state, 'origin_city)
      .agg(sum("count").as("total"))
      .orderBy('origin_state.asc_nulls_last, 'origin_city.asc_nulls_last)
      .show(false)

    /**
     * +------------+-------------+-----+
     * |origin_state|origin_city  |total|
     * +------------+-------------+-----+
     * |CA          |San Diego    |22   |
     * |CA          |San Francisco|21   |
     * |CA          |null         |43   |
     * |NY          |Albany       |17   |
     * |NY          |Elmira       |15   |
     * |NY          |New York     |20   |
     * |NY          |null         |52   |
     * |null        |null         |95   |
     * +------------+-------------+-----+
     */

    line("Cube")

    /**
     * 假设有个 3 列的数据要统计
     * rollup 会返回【column 1】、【column 2】，【column 3】，【column 1，column 2】,【column 1，column 3】，【column 2， column 3】的组合统计
     */

    twoStatesSummary.cube('origin_state, 'origin_city)
      .agg(sum("count").as("total"))
      .orderBy('origin_state.asc_nulls_last, 'origin_city.asc_nulls_last)
      .show(false)

    /**
     * +------------+-------------+-----+
     * |origin_state|origin_city  |total|
     * +------------+-------------+-----+
     * |CA          |San Diego    |22   |
     * |CA          |San Francisco|21   |
     * |CA          |null         |43   |
     * |NY          |Albany       |17   |
     * |NY          |Elmira       |15   |
     * |NY          |New York     |20   |
     * |NY          |null         |52   |
     * |null        |Albany       |17   |
     * |null        |Elmira       |15   |
     * |null        |New York     |20   |
     * |null        |San Diego    |22   |
     * |null        |San Francisco|21   |
     * |null        |null         |95   |
     * +------------+-------------+-----+
     */

    doubleLine("TIme Windows")

    val appleOneYearDF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("src/main/scala/data/stocks/aapl-2017.csv")
    appleOneYearDF.printSchema()

    line("利用 window 函数计算分组后的一周内的平均股价")

    val appleWeeklyAvgDF = appleOneYearDF.groupBy(window('Date, "1 week")).agg(avg("Close").as("weekly_avg"))
    appleWeeklyAvgDF.printSchema()

    //    display the result with ordering by start time and round up to 2 decimal points
    appleWeeklyAvgDF.orderBy("window.start")
      .selectExpr("window.start", "window.end", "round(weekly_avg, 2) as weekly_avg").show(5)

    //    四周的时间窗口，以一周为单位，窗口滑动
    val appleMonthlyAvgDF = appleOneYearDF.groupBy(window('Date, "4 week", "1 week")).agg(avg("Close").as("monthly_avg"))
    appleMonthlyAvgDF.orderBy("window.start").selectExpr("window.start", "window.end", "round(monthly_avg, 2) as monthly_avg").show(5)

    doubleLine("Window Functions")

    val txDataDF = Seq(
      ("John", "2017-07-02", 13.35),
      ("John", "2017-07-06", 27.33),
      ("John", "2017-07-04", 21.72),
      ("Mary", "2017-07-07", 69.74),
      ("Mary", "2017-07-01", 59.44),
      ("Mary", "2017-07-05", 80.14)
    ).toDF("name", "tx_date", "amount")

    val forRankingWindow = Window.partitionBy("name").orderBy(desc("amount"))

    //    给 txDataDF 添加一个新的列用来保存每一行的等级信息。这里需要用 rank 函数来实现等级的计算
    val txDataWithRankDF = txDataDF.withColumn("rank", rank().over(forRankingWindow))

    //    通过rank来过滤每一行，找出前两个rank的信息
    txDataWithRankDF.where('rank < 3).show()

    line("max function")

    //    用 rangeBetween来定义frame的分解，每一个frame包含满足frame的所有行
    val forEntireRangeWindow = Window.partitionBy("name").orderBy(desc("amount")).rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)


    //    遍历amount列，然后计算最大值与每一个amount值的差，得到一个单独的列。
    val amountDifference = max(txDataDF("amount")).over(forEntireRangeWindow) - txDataDF("amount")

    //    添加一个amount_diff列
    val txDiffWithHighestDF = txDataDF.withColumn("amount_diff", round(amountDifference, 3))

    txDiffWithHighestDF.show()

    line()

    val forMovingAvgWindow = Window.partitionBy("name").orderBy("tx_date").rowsBetween(Window.currentRow - 1, Window.currentRow + 1)
    val txMoviingAvgDF = txDataDF.withColumn("moving_avg", round(avg("amount").over(forMovingAvgWindow), 2))
    txMoviingAvgDF.show(false)

    line()

    val forCumulativeSumWindow = Window.partitionBy("name").orderBy("tx_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val txCumulativeSumDF = txDataDF.withColumn("culm_sum", round(sum("amount").over(forCumulativeSumWindow), 2))
    txCumulativeSumDF.show(false)

    line("在SQL中使用Window函数")

    txDataDF.createOrReplaceTempView("tx_data")
    spark.sql("select name, tx_date, amount, rank from (select name, tx_date, amount, RANK() OVER (PARTITION BY name ORDER BY amount DESC) as rank from tx_data) where rank < 3").show(false)

    spark.sql("select name, tx_date, amount, round((max_amount - amount), 2) as amount_diff from (select name, tx_date, amount, MAX(amount) OVER (PARTITION BY name ORDER BY amount DESC RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as max_amount from tx_data)").show(false)

    spark.sql("select name, tx_date, amount, round(moving_avg, 2) as moving_avg from (select name, tx_date, amount, AVG(amount) OVER (PARTITION BY name ORDER BY tx_date ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as moving_avg from tx_data)").show(false)

    spark.sql("select name, tx_date, amount, round(culm_sum, 2) as moving_avg from (select name, tx_date, amount, SUM(amount) OVER (PARTITION BY name ORDER BY tx_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as culm_sum from tx_data)").show(false)

    doubleLine("使用 explain 函数来创建逻辑和物理计划")

    val moviesDF = spark.read.load("src/main/scala/data/movies.parquet")
    val newMoviesDF = moviesDF.filter('produced_year > 1970).withColumn("produced_decade", 'produced_year + 'produced_year % 10).select('movie_title, 'produced_decade).where('produced_decade > 2010)
    newMoviesDF.explain(true)

    line()

    spark.range(1000).filter("id > 100").selectExpr("sum(id)").explain()





    //    ==============================================
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

  case class Student(name: String, gender: String, weight: Long, graduation_year: Long)

  case class Employee(first_name: String, dept_no: Long)

  case class Dept(id: Long, name: String)

  case class Movie(actor_name: String, movie_title: String, produced_year: Long)

  case class Student2(name: String, score: Int)

}
