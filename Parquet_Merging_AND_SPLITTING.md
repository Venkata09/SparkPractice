# How the small files of parquet used to be merged ?

1. When I called merge ? When I called Split ? 

When I called the Restore. 
```scala
if (backupEntry.srcNumFiles <= backupEntry.destNumFiles) {
    merge()
} else {
    split()
}

```


1. What I have done during the merginig. ```text For the merge operartion I am using the coalesce. ```
1. What I have done during the Splitting. ```text For the Split operation I am using the REPARTITION. ```



ParquetS3Maker supports

Parquet Merging, Splitting and Saving using Apache Spark to S3 (or other Spark Supported FileSystems)


Merging - combine many parquet files to fewer parquet files
Splitting - split fewer parquet files to many parquet files
Backup - from a BackupManifest, save parquet files from src directory to destination directory
Restore - from a BackupManifest, restore parquet files from destination directory to src directory

**Partitioning the data as well **
