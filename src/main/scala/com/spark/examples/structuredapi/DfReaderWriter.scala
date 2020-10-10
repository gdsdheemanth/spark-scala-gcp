package com.spark.examples.structuredapi

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/**
 * usage
 * spark-submit --master local[*] --class com.spark.examples.structuredapi.DfReaderWriter "C:\Users\Dheemanth Garikipati\Desktop\SparkSimpleApp\target\SparkSimpleApp-1.0-SNAPSHOT.jar" "C:\data\sf-fire-calls.csv"
 */
object DfReaderWriter {
  def main(args: Array[String]): Unit ={
    if(args.length <1){
      println("required the file path")
      sys.exit(1)
    }
    val file = args(0)
    val output  = args(1)
    // we will create a spark session to read the file
    val spark = SparkSession.builder
      .master("local")
      .appName("dfreaderandwriter")
      .getOrCreate()

    //we will define schema for the file
    val schema = StructType(Array(StructField("CallNumber", IntegerType, true),
      StructField("UnitID", StringType, true),
      StructField("IncidentNumber", IntegerType, true),
      StructField("CallType", StringType, true),
      StructField("CallDate", StringType, true),
      StructField("WatchDate", StringType, true),
      StructField("CallFinalDisposition", StringType, true),
      StructField("AvailableDtTm", StringType, true),
      StructField("Address", StringType, true),
      StructField("City", StringType, true),
      StructField("Zipcode", IntegerType, true),
      StructField("Battalion", StringType, true),
      StructField("StationArea", StringType, true),
      StructField("Box", StringType, true),
      StructField("OriginalPriority", StringType, true),
      StructField("Priority", StringType, true),
      StructField("FinalPriority", IntegerType, true),
      StructField("ALSUnit", BooleanType, true),
      StructField("CallTypeGroup", StringType, true),
      StructField("NumAlarms", IntegerType, true),
      StructField("UnitType", StringType, true),
      StructField("UnitSequenceInCallDispatch", IntegerType, true),
      StructField("FirePreventionDistrict", StringType, true),
      StructField("SupervisorDistrict", StringType, true),
      StructField("Neighborhood", StringType, true),
      StructField("Location", StringType, true),
      StructField("RowID", StringType, true),
      StructField("Delay", FloatType, true)))

    // we will read the file using dataframe reader
    val df = spark.read.schema(schema).option("header",true).csv(file)
    df.show(5)

    df.write.format("parquet").save(output)



  }

}
