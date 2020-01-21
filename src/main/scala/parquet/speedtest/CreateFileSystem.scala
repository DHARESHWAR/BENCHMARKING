package parquet.speedtest

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

/*
  1. Time Efficiency for Creating file System on Azure Blob
  args(0) : number of records you want to insert, [50000, 100000, 200000 etc]
*/
object CreateFileSystem extends App {

  var number = args(0)
  println("number ::" + number)
  val conf = new SparkConf()
    .setMaster("yarn")
    .setAppName("app-name")
    .set("spark.hadoop.parquet.enable.summary-metadata", "false")
    .set("spark.sql.parquet.mergeSchema", "true").set("spark.sql.parquet.filterPushdown", "true").set("spark.sql.orc.impl", "native").set("spark.sql.orc.filterPushdown", "true").set("spark.sql.orc.splits.include.file.footer", "true").set("spark.sql.orc.cache.stripe.details.size", "10000").set("spark.sql.hive.metastorePartitionPruning", "true").set("spark.sql.sources.partitionOverwriteMode", "dynamic")

  val sparkSession = SparkSession.builder().config(conf).getOrCreate()
  val sc = sparkSession.sparkContext

  sc.hadoopConfiguration.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
  sc.hadoopConfiguration.set("fs.azure.account.keyprovider.storage_account.blob.core.windows.net", "org.apache.hadoop.fs.azure.SimpleKeyProvider")
  sc.hadoopConfiguration.set("fs.azure.account.key.storage_account.blob.core.windows.net", "storage_account_secret_key")

  val PARQUET = "parquet"
  val ORC = "orc"
  val compressionCodec = "org.apache.hadoop.io.compress.GzipCodec" // org.apache.hadoop.io.compress.SnappyCodec
  val TIME_FORMAT: String = "%2.2f"

  val tempLocation = System.nanoTime()
  val blobLocation = "wasb://container-name@storage-account-name.blob.core.windows.net/directory-name/file_name"

  val PARQUET_WRITE_LOCATION = "wasb://container-name@storage-account-name.blob.core.windows.net/parquet/directory-name/"
  val ORC_WRITE_LOCATION = "wasb://container-name@storage-account-name.blob.core.windows.net/orc/directory-name/"

  /*
        Generate Dataset on N Records
  */
  var dataSet: Dataset[Row] = GenerateDataset.getDataset(number.toInt, sparkSession)

  val parquetStartTime: Long = System.nanoTime()
  dataSet.write.format(PARQUET).option("codec", compressionCodec).mode(SaveMode.Overwrite).save(PARQUET_WRITE_LOCATION)
  val parquetEndTime: Long = System.nanoTime()
  println(PARQUET_WRITE_LOCATION)

  val orcStartTime: Long = System.nanoTime()
  dataSet.write.format(ORC).option("codec", compressionCodec).mode(SaveMode.Overwrite).save(ORC_WRITE_LOCATION)
  val orcEndTime: Long = System.nanoTime()
  println(ORC_WRITE_LOCATION)

  println("All Time is in seconds")
  println("PARQUET Creation Time:")
  println(TIME_FORMAT.format((parquetEndTime - parquetStartTime) / 1000000000.0))

  println("ORC Creation Time:")
  println(TIME_FORMAT.format((orcEndTime - orcStartTime) / 1000000000.0))

}
