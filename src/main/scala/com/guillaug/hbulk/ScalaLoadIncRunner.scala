package com.guillaug.hbulk

import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.io.hfile.{HFile, Compression}
import org.apache.hadoop.hdfs.DistributedFileSystem
import java.net.URI
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, KeyValue}
import org.joda.time.DateTime
import org.apache.hadoop.hbase.client.{HTable, HBaseAdmin}
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles

/**
 * Created with IntelliJ IDEA.
 * User: guillaumeaugais
 * Date: 7/12/12
 * Time: 3:09 PM
 * To change this template use File | Settings | File Templates.
 */
case class SimpleKeyValue(row: Array[Byte], value: Array[Byte])

class ScalaLoadIncRunner {

  val TABLE = Bytes.toBytes("hfiletable")
  val CFAMILY = Bytes.toBytes("hfamily")
  val QUAL = Bytes.toBytes("hqual")

  val conf: Configuration = {
    val configuration = new Configuration()
    configuration.set("fs.default.name", "hdfs://localhost:9000")
    configuration
  }

  val BLOCKSIZE = 64*1024
  val COMPRESSION = Compression.Algorithm.NONE.getName

  def load(data: List[SimpleKeyValue]) {

    val dfs = new DistributedFileSystem()
    dfs initialize(new URI(conf.get("fs.default.name")), conf)

    val dir = new Path(conf.get("fs.default.name") + "/hbase/tmp/hfile")
    dir.makeQualified(dfs)
    val familyDir = new Path(dir, Bytes.toString(CFAMILY))

    createHFile(dfs, new Path(familyDir, "hfile_000"), CFAMILY, QUAL, data)

    try {
      val hBaseAdmin = new HBaseAdmin(conf)
      val htd = new HTableDescriptor(TABLE)
      htd addFamily new HColumnDescriptor(CFAMILY)

      if (!hBaseAdmin.tableExists(TABLE)) hBaseAdmin.createTable(htd)
      val table = new HTable(TABLE)

      while(!hBaseAdmin.isTableAvailable(TABLE)) Thread.sleep(500)

      val loader = new LoadIncrementalHFiles(conf)
      loader.doBulkLoad(dir, table)

    } finally {
      println("... Bulk Load Sucessfull !")
    }
  }

  def createHFile(fs: FileSystem, path: Path,
                  CFAMILY: Array[Byte], QUAL: Array[Byte], data: List[SimpleKeyValue]): Unit = {
    val writer = new HFile.Writer(
      fs, path, BLOCKSIZE, COMPRESSION, KeyValue.KEY_COMPARATOR)
    val now = new DateTime().getMillis

    try {
      data foreach { simpleKv =>
        val kv = new KeyValue(simpleKv.row, CFAMILY, QUAL, now, simpleKv.value)
        writer append kv
      }
    } finally {
      writer.close()
    }
  }
}

object ScalaLoadIncRunner {

  def main(args: Array[String]) {
    new ScalaLoadIncRunner().load(
      List(
        SimpleKeyValue(Bytes.toBytes("myFirstHFileRow"), Bytes.toBytes("Finally..."))
      )
    )
  }
}
