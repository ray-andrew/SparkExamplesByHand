package com.andrew.repartition

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.spark_partition_id

object RangePartitionExample extends App {

  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder()
    .master("local[3]")
    .appName("Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  val conf = spark.sparkContext.hadoopConfiguration
  val fs = org.apache.hadoop.fs.FileSystem.get(conf)
  fs.setVerifyChecksum(false)

  import spark.implicits._

  // For implicit conversions from RDDs to DataFrames
  private val df = Seq(
    (10, "order 1001", 10d),
    (11, "order 1002", 240d),
    (11, "order 1001", 13d),
    (12, "order 1001", 12d),
    (12, "order 1003", 232d),
    (13, "order 1004", 100d),
    (13, "order 1001", 14d),
    (13, "order 1001", 12d),
    (14, "order 1005", 11d),
    (15, "order 1006", 20d),
    (16, "order 1007", 390d),
    (17, "order 1008", 30d),
    (18, "order 1009", 99d),
    (19, "order 1010", 55d),
    (20, "order 1011", 129d),
    (21, "order 1012", 75d),
    (22, "order 1013", 173d),
    (22, "order 1013", 12d),
    (22, "order 1013", 14d),
    (10, "order 1001", 11d),
    (10, "order 1001", 12d)
  ).toDF("id", "name", "amount")

  df.show()

  val repart_df = df
    .repartitionByRange(3, $"id")
    .withColumn("partition_id", spark_partition_id())

  repart_df.show()

//  repart_df.write.mode(SaveMode.Overwrite).json("./output/range_part_people")

  val savePath = "./output/range_part_people"
  repart_df.write.mode(SaveMode.Overwrite).parquet(savePath)

  // collect all id in each partiitons
  val res = repart_df
    .mapPartitions(rows => {
      val idsInPartition = rows.map(row => row.getAs[Int]("id")).toSeq.sorted.mkString(",")
      Iterator(idsInPartition)
    })
    .collect()

  //  println(res.toSeq)

  // TimeUnit.MINUTES.sleep(10)

}
