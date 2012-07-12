/**
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.guillaug.hbulk;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.joda.time.DateTime;

public class LoadIncRunner {

    private static final byte[] TABLE = Bytes.toBytes("ct");
    private static final byte[] QUALIFIER = Bytes.toBytes("cq");
    private static final byte[] FAMILY = Bytes.toBytes("cf");

    private static Configuration conf = new Configuration();

    static {
        conf.set("fs.default.name", "hdfs://localhost:9000");
    }

    public static int BLOCKSIZE = 64*1024;
    public static String COMPRESSION =
            Compression.Algorithm.NONE.getName();

    public static void main(String[] args) throws Exception {
        runLoad("SimpleLoad",
                new byte[][][] {
                        new byte[][]{ Bytes.toBytes("row-aa"), Bytes.toBytes("value-aa") }
                });
    }

    private static void runLoad(String loadName, byte[][][] hfileRanges)
            throws Exception {
        System.out.println(loadName + ": creating HFiles...");

        // TO DO : FileSystem fs should be taken from conf and work for hdfs, and local
        // without custom code
        DistributedFileSystem fs = new DistributedFileSystem();
        fs.initialize(new URI("hdfs://localhost:9000"), conf);

        Path dir = new Path("hdfs://localhost:9000/hbase/tmp/hfile/");
        dir.makeQualified(fs);
        Path familyDir = new Path(dir, Bytes.toString(FAMILY));

        int hfileIdx = 0;
        for(byte[][] kvArray: hfileRanges) {
            byte[] key = kvArray[0];
            byte[] value = kvArray[1];
            createHFile(fs, new Path(familyDir, "hfile_" + hfileIdx++),
                    FAMILY, QUALIFIER, key, value);
        }

        try {
            HBaseAdmin admin = new HBaseAdmin(conf);
            HTableDescriptor htd = new HTableDescriptor(TABLE);
            htd.addFamily(new HColumnDescriptor(FAMILY));

            if (!admin.tableExists(TABLE)) admin.createTable(htd);
            HTable table = new HTable(TABLE);

            while(!admin.isTableAvailable(TABLE)) Thread.sleep(500);

            LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
            loader.doBulkLoad(dir, table);

        } finally {
            System.out.println("... OK !");
        }
    }

    static void createHFile(
            FileSystem fs, Path path,
            byte[] family, byte[] qualifier,
            byte[] key, byte[] value) throws IOException
    {
        HFile.Writer writer = new HFile.Writer(
                fs, path, BLOCKSIZE, COMPRESSION, KeyValue.KEY_COMPARATOR);

        try {
            long now = new DateTime().getMillis();
            KeyValue kv = new KeyValue(key, family, qualifier, now, value);
            writer.append(kv);
        } finally {
            writer.close();
        }
    }
}