package com.andrew

object RddToDataframe extends App {

  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder()
    .master("local[3]")
    .appName("Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
  // For implicit conversions from RDDs to DataFrames

  import spark.implicits._

  case class Person(name: String, age: Int)

  // Create an RDD of Person objects from a text file, convert it to a Dataframe
  val peopleDF = spark.sparkContext
    .textFile("./data/people.txt")
    .map(_.split(","))
    .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
    .toDF()
  // Register the DataFrame as a temporary view
  peopleDF.createOrReplaceTempView("people")

  // SQL statements can be run by using the sql methods provided by Spark
  val teenagersDF = spark.sql(
    """
      |SELECT name, age FROM people
      |WHERE age BETWEEN 13 AND 19
      |
      |""".stripMargin)

  // The columns of a row in the result can be accessed by field index
  teenagersDF.map(teenager => "Name: " + teenager(0)).show()
  // +------------+
  // |       value|
  // +------------+
  // |Name: Justin|
  // +------------+

  // teenagersDF.map(teenager => ("name: "+teenager(0), teenager(1))).show()

  // or by field name
  teenagersDF.map(teenager => "Name: " + teenager.getAs[String]("name")).show()
  // +------------+
  // |       value|
  // +------------+
  // |Name: Justin|
  // +------------+

  // No pre-defined encoders for Dataset[Map[K,V]], define explicitly
  implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
  // Primitive types and case classes can be also defined as
  // implicit val stringIntMapEncoder: Encoder[Map[String, Any]] = ExpressionEncoder()

  // row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
  teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect()
  // Array(Map("name" -> "Justin", "age" -> 19))
}
