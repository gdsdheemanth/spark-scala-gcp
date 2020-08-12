package com.spark.example


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, Metadata, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{expr,col,column}
import org.apache.spark.sql.Row

object FlightsDataProcess {

  def processData(spark:SparkSession):Unit ={
    val df = spark.read.option("header",true)
      .option("inferschema",true)
      .format("json")
      .load("C:\\Users\\Dheemanth Garikipati\\Desktop\\data\\flight-data\\json\\2010-summary.json")
    df.show(5)

    /*
    val schema = StructType(Array(StructField("DEST_COUNTRY_NAME",StringType,true),
                                  StructField("ORIGIN_COUNTRY_NAME",StringType,true),
                                  StructField("count",LongType,true,Metadata.fromJson("{\"hello\":\"world\"}"))))

    val df2 = spark.read.format("json").option("header",false).schema(schema).load("C:\\Users\\Dheemanth Garikipati\\Desktop\\data\\flight-data\\json\\2010-summary.json")

    df2.show(5)

     */

    df.createOrReplaceTempView("flights")
//    spark.sql("select * from flights limit 10")

//    df.select("DEST_COUNTRY_NAME","count").show(3)
    /*
    df.select(
      df.col("count"),
      col("count"),
      column("count"),
      expr("count")
    ).show(5)
     */
    df.drop("DEST_COUNTRY_NAME").show(5)


  }
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    println("*************************")
    println(spark)
    println("*************************")
    processData(spark)
    spark.close()
  }

}
