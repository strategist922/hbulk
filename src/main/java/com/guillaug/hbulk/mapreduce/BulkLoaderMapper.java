package com.guillaug.hbulk.mapreduce;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.joda.time.DateTime;

import java.io.IOException;

public class BulkLoaderMapper extends Mapper<Text, Text, ImmutableBytesWritable, Put> {

    protected byte[] getHBaseRowKeyBytes(Text key) throws IOException{
        return Bytes.toBytes(key.toString());
    }

    protected byte[] getHBaseFamilyNameBytes(){
        return Bytes.toBytes("myfam");
    }

    protected byte[] getHBaseQualifierNameBytes(){
        return Bytes.toBytes("myqual");
    }

    protected byte[] getHBaseValueBytes(Text val) throws IOException{
        return Bytes.toBytes(val.toString());
    }

    protected void map(Text key, Text val, Context context) throws IOException, InterruptedException {
        System.out.println(key.toString());
        Put put = new Put(getHBaseRowKeyBytes(key));
        KeyValue kv = new KeyValue(
                getHBaseRowKeyBytes(key), getHBaseFamilyNameBytes(),
                getHBaseQualifierNameBytes(), System.currentTimeMillis(), getHBaseValueBytes(val));
        put.add(kv);
        context.write(new ImmutableBytesWritable(put.getRow()), put);
    }
}