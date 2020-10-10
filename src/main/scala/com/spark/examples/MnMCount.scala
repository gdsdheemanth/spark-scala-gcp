package com.spark.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}

/**
 * spark-submit --master local[*] --class com.spark.example.MnMCount "C:\Users\Dheemanth Garikipati\Desktop\SparkSimpleApp\target\SparkSimpleApp-1.0-SNAPSHOT.jar" "C:\data\mnmdataset.csv"
 */

object MnMCount {
  def main(args: Array[String]): Unit = {
    if(args.length <1){
      println(
        """
          |usage spark-submit
          |--master local[*]
          |--class com.spark.example.MnMCount
          |"C:\Users\Dheemanth Garikipati\Desktop\SparkSimpleApp\target\SparkSimpleApp-1.0-SNAPSHOT.jar"
          |"C:\data\mnmdataset.csv"
          |""".stripMargin)
      sys.exit(1)
    }

    //creating the spark session
    val spark = SparkSession.builder
      .appName("mnmcount")
      .master("local")
      .getOrCreate()
    val mnmFile = args(0)

    spark.conf.set("spark.sql.shuffle.partitions", 6)

    spark.sparkContext.setLogLevel("ERROR")
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    val mnmDf = spark.read.format("csv").option("header","true")
      .option("inferSchema","true")
      .load(mnmFile)

    val countMnMDF = mnmDf.select("state","color","count")
      .groupBy("state","color")
      .agg(count("count").alias("total"))
      .orderBy(desc("total"))

    countMnMDF.show(60)

    println(s"Total rows = ${countMnMDF.count()}")
  }
}
