package com.andrew.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.spark.internal.Logging


object ParquetMetaReaderExample extends App{

  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder()
    .master("local[3]")
    .appName("Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
  spark.sparkContext.setLogLevel("warn")
  val conf = spark.sparkContext.hadoopConfiguration

  ParquetMetaReader(conf, "./data/part-0.snappy.parquet")
    .read()

}
object ParquetMetaReader {
  def apply(conf:Configuration, file:String):ParquetMetaReader = {
   new ParquetMetaReader(conf, file)
  }

}

class ParquetMetaReader(conf:Configuration, file:String) extends Logging{
  def read(): Unit = {
    val footer = ParquetFileReader.readFooter(conf, qualifiedPath(file), ParquetMetadataConverter.NO_FILTER)
    logWarning(s"footer meta: ${footer}")
  }

  def qualifiedPath(fileName:String):Path ={
    val path = new Path(".")
    val fs = path.getFileSystem(conf)
    val cwd = fs.makeQualified(path)
    new Path(fileName).makeQualified(fs.getUri, cwd)
  }
}
