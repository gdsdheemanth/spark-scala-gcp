package com.spark.examples.structuredapi

import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, StringToAttributeConversionHelper}
import org.apache.spark.sql.functions.{col, column, concat, expr}
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.{Row, SparkSession}
/**
 * Sparks basic datatypes
 */
import org.apache.spark.sql.types.{IntegerType, StringType}

/**
 * Sparks complex data types
 */

import org.apache.spark.sql.types.{StructField, StructType,ArrayType,MapType}

object SparkDataTypes {
  def main(args: Array[String]): Unit ={
  /**
   * There are two ways to define a schema
   * 1.through dataframe Api
   * 2.By DDl statement
   */

    val ddlSchema = "Id INT, First STRING, Last STRING, Url STRING,Published STRING, Hits INT, Campaigns ARRAY<STRING>"

    val spark = SparkSession
      .builder
      .appName("Example")
      .getOrCreate()
    if (args.length <= 0) {
      println("spark-submit --master local[*] --class com.spark.examples.structuredapi.SparkDataTypes \"C:\\Users\\Dheemanth Garikipati\\Desktop\\SparkSimpleApp\\target\\SparkSimpleApp-1.0-SNAPSHOT.jar\" \"C:\\data\\blogs.json\"")
      System.exit(1)
    }
    // Get the path to the JSON file
    val jsonFile = args(0)
    // Define our schema programmatically
    val schema = StructType(Array(StructField("Id", IntegerType, false),
      StructField("First", StringType, false),
      StructField("Last", StringType, false),
      StructField("Url", StringType, false),
      StructField("Published", StringType, false),
      StructField("Hits", IntegerType, false),
      StructField("Campaigns", ArrayType(StringType), false)))
    // Create a DataFrame by reading from the JSON file
    // with a predefined schema
    val blogsDF = spark.read.schema(schema).json(jsonFile)
    // Show the DataFrame schema as output
    blogsDF.show(false)
    // Print the schema
    println(blogsDF.printSchema)
    println(blogsDF.schema)

    blogsDF.columns
    blogsDF.col("Id")

    blogsDF.select(expr("Hits * 5")).show(2)
    blogsDF.select(col("hits")*5).show(2)

    blogsDF.withColumn("bighits" ,(expr("Hits > 1000"))).show(2)

    blogsDF.withColumn("author_id",concat(expr("First"),expr("Id")))
      .select("author_id").show(4)

    blogsDF.sort(col("id").desc).show()

}
}