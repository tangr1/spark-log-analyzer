package com.ctofunds.dd

import org.apache.hadoop.conf.Configuration

object HadoopConfiguration {
  def configure(args: Array[String], configuration: Configuration): Configuration = {
    configuration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
    configuration.set("mapred.input.dir.recursive", "true")
    configuration.set("hive.mapred.supports.subdirectories", "true")
    configuration.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    configuration.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
    configuration.set("fs.s3n.awsAccessKeyId", args(0))
    configuration.set("fs.s3.awsAccessKeyId", args(0))
    configuration.set("fs.s3n.awsSecretAccessKey", args(1))
    configuration.set("fs.s3.awsSecretAccessKey", args(1))
    configuration
  }
}
