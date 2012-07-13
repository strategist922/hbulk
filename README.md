# Installing

You will need to have a working installation of hadoop-0.20.2-cdh3u4 & hbase-0.90.6-cdh3u4

# Launching 

## MapReduce BulkLoader 

You need to package the code into a jar and add it to the classpath before launching the main of BulkLoader

```
sbt package
sbt package && cp target/scala-2.9.1/scala-hbulk_2.9.1-0.1.jar lib/ && hadoop fs -rmr /hbase/bulk/output && sbt
// sbt loads...
```

You can then launch the main in sbt
```
// in sbt console
run-main com.guillaug.hbulk.mapreduce.BulkLoader
```