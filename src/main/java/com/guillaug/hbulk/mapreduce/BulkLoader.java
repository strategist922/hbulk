package com.guillaug.hbulk.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.hadoopbackport.TotalOrderPartitioner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.joda.time.DateTime;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

public class BulkLoader extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new BulkLoader(), args);
        System.exit(exitCode);

    }

    @Override
    public int run(String[] otherArgs) throws Exception {

        Configuration config = (Configuration) getConf();//new Configuration();

        config.set("fs.default.name", "hdfs://localhost:9000");
        config.set("dfs.replication", "1");
        config.set("mapred.job.tracker", "localhost:9001");

        config.set("kv-text-input", "hdfs://localhost:9000/hbase/bulk/input.txt");
        config.set("output-path", "hdfs://localhost:9000/hbase/bulk/output");
        config.set("bulk-load-table", "bulkloadtable");
        config.set("family", "myfam");
        config.set("column", "mycol");
        config.set("input-format", "column");

        HBaseAdmin admin = new HBaseAdmin(config);
        HTableDescriptor htd = new HTableDescriptor(config.get("bulk-load-table"));
        htd.addFamily(new HColumnDescriptor(config.get("family")));

        if (!admin.tableExists(config.get("bulk-load-table"))) admin.createTable(htd);

        while(!admin.isTableAvailable(config.get("bulk-load-table"))) Thread.sleep(500);

        Job job = new Job(config);

        //job.setOutputKeyClass(ImmutableBytesWritable.class);
        //job.setOutputValueClass(Put.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        job.setMapperClass(BulkLoaderMapper.class);

        job.setJarByClass(BulkLoaderMapper.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        for(Map.Entry<String, String> entry: job.getConfiguration()) {
            System.out.printf("%s=%s\n", entry.getKey(), entry.getValue());
        }

        KeyValueTextInputFormat.addInputPath(job, new Path(config.get("kv-text-input")));
        job.setOutputFormatClass(HFileOutputFormat.class);

        job.setPartitionerClass(TotalOrderPartitioner.class);

        Configuration hConfig = HBaseConfiguration.create(config);
        hConfig.setLong("version", System.currentTimeMillis());
        // quorum ?
        job.setJobName("Bulk Loading table: " + hConfig.get("bulk-load-table","YOU HAVE NOT SET bulk-load-table PARAMETER"));

        TableMapReduceUtil.addDependencyJars(job);

        HFileOutputFormat.setOutputPath(job, new Path(config.get("output-path")));
        HFileOutputFormat.configureIncrementalLoad(job, new HTable(hConfig, config.get("bulk-load-table")));


        job.waitForCompletion(true);

        HTable table = new HTable(config.get("bulk-load-table"));

        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(config);
        loader.doBulkLoad(new Path(config.get("output-path")), table);

        return 0; //normal exit

    }
}