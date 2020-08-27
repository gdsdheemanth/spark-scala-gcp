package com.spark.aws.examples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object S3IOTest {
  def main (arg: Array[String]): Unit = {

    val spark = SparkSession.builder().
      master("local").
      appName("demoApp").
      getOrCreate()
    val accessKeyId = System.getenv("AWS_ACCESS_KEY_ID")
    val secretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.awsAccessKeyId", accessKeyId)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.awsSecretAccessKey", secretAccessKey)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.sparkContext.setLogLevel("ERROR")
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    println("****************************")
    println(s"spark session create: $spark")
    println("****************************")
    val data = spark.read.option("header",true).option("inferschema",true).csv("s3a://examplelake/sparkinput/*.csv")
    data.show(5)
    data.createOrReplaceTempView("flights")
    spark.sql("select * from flights limit 10").show()
    data.write.mode("overwrite").format("csv").save("s3a://publicexamplelake/sparkoutput/out/")
    spark.stop()
  }
}
