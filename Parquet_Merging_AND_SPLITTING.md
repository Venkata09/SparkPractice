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

