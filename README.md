# BENCHMARKING
Code repository for Benchmarking PARQUET vs ORC file system performance

We are using Azure Blob Storage for keeping Parquet and orc files.

1. Time Efficiency for Creating file System on Azure Blob
    CreateFileSystem.scala Writes Random N Employee records and write to Parquet and orc file system.
    It measures time takes to create Parquet and orc file system.
    
2. Space Efficiency
   Used Hadoop command to figure out the space used by Parquet and orc file system.
   
  hadoop fs -du -s -h  wasb://container-name@storage-account-name.blob.core.windows.net/parquet/directory-name/
  hadoop fs -du -s -h  wasb://container-name@storage-account-name.blob.core.windows.net/orc/directory-name/
  
3. Time Efficiency in finding count
    CountSpeed.scala measure Time Taken to count the number of records from Parquet and orc file system.
    
4. Time Efficiency in running complex query
    RunSql.scala measure Time Taken to run complex query on Parquet and orc file system.
   
  
    
