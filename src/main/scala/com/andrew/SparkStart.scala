package com.andrew

object SparkStart extends App {

  println("hell, spark")

  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder()
    .master("local[3]")
    .appName("Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  val readPath = "./data/people.json"
  val df = spark.read.json("./data/people.json")

  // Displays the content of the DataFrame to stdout
  df.show()
  // +----+-------+
  // | age|   name|
  // +----+-------+
  // |null|Michael|
  // |  30|   Andy|
  // |  19| Justin|
  // +----+-------+


  import spark.implicits._

  df.printSchema()

  df.select("name").show()

  df.select($"name", $"age" + 1).show()

  df.select($"name"+"_name", $"age"+1).show()

  df.filter($"age" > 21).show()

  df.groupBy($"age").count().show()


  // Register the DataFrame as a SQL temporary view
  df.createOrReplaceTempView("people")

  val sqlDF = spark.sql("SELECT * FROM people")
  sqlDF.show()
  // +----+-------+
  // | age|   name|
  // +----+-------+
  // |null|Michael|
  // |  30|   Andy|
  // |  19| Justin|
  // +----+-------+

  // GLobal view
  // Register the DataFrame as a global temporary view
  df.createGlobalTempView("people")

  // Global temporary view is tied to a system preserved database `global_temp`
  spark.sql("SELECT * FROM global_temp.people").show()
  // +----+-------+
  // | age|   name|
  // +----+-------+
  // |null|Michael|
  // |  30|   Andy|
  // |  19| Justin|
  // +----+-------+

  // Global temporary view is cross-session
  spark.newSession().sql("SELECT * FROM global_temp.people").show()

  // Creating dataset
  case class Person(name: String, age: Int)

  val caseClassDS = Seq(Person("Andy", 32)).toDF()

  val primDS = Seq(1, 2, 3).toDS()
  primDS.map(_ + 1).collect()


  val peopleDS = spark.read.json(readPath).as[Person]
  peopleDS.show()

}
