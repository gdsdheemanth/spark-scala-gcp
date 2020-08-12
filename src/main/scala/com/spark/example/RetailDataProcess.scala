package com.spark.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col}
import org.apache.spark.sql.functions.{expr, pow,corr,current_date,current_timestamp}


object RetailDataProcess {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("demo").master("local").getOrCreate()
    print(spark)
    println("**********process start**********")
    val df = spark.read.format("csv").
      option("inferschema",true).
      option("header",true).
      load("C:\\Users\\Dheemanth Garikipati\\Desktop\\data\\retail-data\\by-day\\2010-12-01.csv")

    df.createOrReplaceTempView("retaildata")
    df.printSchema()
    df.show(5)

    df.where(col("InvoiceNo").equalTo(536365)).select("Description","InvoiceNo").count()

    val fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5
//    df.select(expr("CustomerId").cast("Int"), fabricatedQuantity.alias("realQuantity")).show(2)
//    df.selectExpr("CustomerId","POWER((Quantity * UnitPrice),2.0)+ 5 as fabricatedprice").show(5)
//    df.select(corr("Quantity", "UnitPrice") as "corilation").show()
//    val cointainsblack = col("Description").contains("BLACK")
//    val cointainswhite = col("Description").contains("WHITE")
//    df.withColumn("simplecolor",cointainsblack.or(cointainswhite)).where("simplecolor")
//      .select("Description").show(5)

//    val datedf = spark.range(5).withColumn("today",current_date()).withColumn("now",current_timestamp())
//
//    datedf.createOrReplaceTempView("datetable")
//
//    datedf.show()

    df.na.drop("all")
  }

}
