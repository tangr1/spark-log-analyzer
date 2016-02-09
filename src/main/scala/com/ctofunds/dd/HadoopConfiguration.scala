package com.ctofunds.dd

import org.apache.hadoop.conf.Configuration

object HadoopConfiguration {
  def configure(configuration: Configuration): Configuration = {
    configuration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
    configuration.set("mapred.input.dir.recursive", "true")
    configuration.set("hive.mapred.supports.subdirectories", "true")
    /*
    configuration.set("fs.s3n.awsAccessKeyId", sys.env("AWS_ACCESS_KEY_ID"))
    configuration.set("fs.s3.awsAccessKeyId", sys.env("AWS_ACCESS_KEY_ID"))
    configuration.set("fs.s3n.awsSecretAccessKey", sys.env("AWS_SECRET_ACCESS_KEY"))
    configuration.set("fs.s3.awsSecretAccessKey", sys.env("AWS_SECRET_ACCESS_KEY"))
    */
    return configuration
  }
}
