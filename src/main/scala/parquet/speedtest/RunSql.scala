package parquet.speedtest

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/*
  4. Time Efficiency in running complex query
   args(0) : Parquet file Location in Azure Blob. Example : wasb://container-name@storage-account-name.blob.core.windows.net/parquet/directory-name/
   args(1) : Orc file Location in Azure Blob. Example : wasb://container-name@storage-account-name.blob.core.windows.net/orc/directory-name/

*/

object RunSql extends App {
  var parquetFileLocation = args(0)
  var orcFileLocation = args(1)

  val PARQUET = "parquet"
  val ORC = "orc"
  val compressionCodec = "org.apache.hadoop.io.compress.GzipCodec" // org.apache.hadoop.io.compress.SnappyCodec
  val TIME_FORMAT: String = "%2.2f"

  val PARQUET_WRITE_LOCATION = parquetFileLocation
  val ORC_WRITE_LOCATION = orcFileLocation


  val conf = new SparkConf().setMaster("yarn").setAppName("app-name").set("spark.hadoop.parquet.enable.summary-metadata", "false").set("spark.sql.parquet.mergeSchema", "true").set("spark.sql.parquet.filterPushdown", "true").set("spark.sql.orc.impl", "native").set("spark.sql.orc.filterPushdown", "true").set("spark.sql.orc.splits.include.file.footer", "true").set("spark.sql.orc.cache.stripe.details.size", "10000").set("spark.sql.hive.metastorePartitionPruning", "true").set("spark.sql.sources.partitionOverwriteMode", "dynamic")

  val sparkSession = SparkSession.builder().config(conf).getOrCreate()
  val sc = sparkSession.sparkContext

  sc.hadoopConfiguration.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
  sc.hadoopConfiguration.set("fs.azure.account.keyprovider.storage_account.blob.core.windows.net", "org.apache.hadoop.fs.azure.SimpleKeyProvider")
  sc.hadoopConfiguration.set("fs.azure.account.key.storage_account.blob.core.windows.net", "storage_account_secret_key")


  val orcStartTime: Long = System.nanoTime()
  val employeeOrc: Dataset[Row] = sparkSession.read.format(ORC).load(ORC_WRITE_LOCATION)
  employeeOrc.createOrReplaceTempView("employeeOrc")
  sparkSession.sql("select count(*) from employeeOrc where (team='REPORTING' and project='DATA-ANALYTICS') or (team='FINANCE' and project='DASHBOARD')  ").show()
  val orcEndTime: Long = System.nanoTime()
  println(TIME_FORMAT.format((orcEndTime - orcStartTime) / 1000000000.0))

  val parquetStartTime: Long = System.nanoTime()
  val employeeParquet: Dataset[Row] = sparkSession.read.format(PARQUET).load(PARQUET_WRITE_LOCATION)
  employeeParquet.createOrReplaceTempView("employeeParquet")
  sparkSession.sql("select count(*) from employeeParquet where (team='REPORTING' and project='DATA-ANALYTICS') or (team='FINANCE' and project='DASHBOARD')  ").show()
  val parquetEndTime: Long = System.nanoTime()
  println(TIME_FORMAT.format((parquetEndTime - parquetStartTime) / 1000000000.0))

}
