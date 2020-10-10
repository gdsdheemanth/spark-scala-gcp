package com.spark.examples

import org.apache.spark.sql.SparkSession

object CreatingSparkSession {
  def main(args: Array[String]): Unit = {
    //creating a spark session
    val warehouseLocation = "file:${system:user.dir}/spark-warehouse"
    val spark = SparkSession.builder
      .master("local")
      .appName("demo")
      .config("spark.sql.warehouse.dir", warehouseLocation)
//      .enableHiveSupport()
      .getOrCreate()

    //set new runtime options
    spark.conf.set("spark.sql.shuffle.partitions", 6)
//    spark.conf.set("spark.executor.memory", "2g")
    //get all settings
    val configMap:Map[String, String] = spark.conf.getAll
    configMap.foreach(println)

    //  to display the metadata from catalog
    spark.catalog.listDatabases.show(false)
    spark.catalog.listTables.show(false)

    //stoping the spark session
    spark.close()


  }

}
