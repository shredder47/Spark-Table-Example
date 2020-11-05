package com.shredder.example

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


object Example1 extends App {
  /**
   * SPARK - HIVE -
   *
   * Spark can create two types of table -> 1. Managed
   * 2. Unmanaged (External Table)
   *
   * For managed, Spark does two things, one it stores metadata to Hive MetaStore and the phisical data is stored at predefined location 'spark.sql.warehouse.dir' or by default its the project folder
   *
   * For External , Spark uses same Metadata store but have to specify the datastore location(stored at some other directory)
   *
   * DROPPIING -> FOr managed, everything gets deleted, for unmanaged only matadata is dropped , data is untouched as its outside
   *
   *
   *
   */
  val sparkSession: SparkSession = SparkSession
    .builder
    .appName("Demo")
    .config("spark.sql.warehouse.dir", "/home/shredder/Desktop/warehouse") // For manual data-store location
    .enableHiveSupport() // To enable managed features i.e both catalog (persistant hive metastore) and data storage by spark
    .master("local[*]")
    .getOrCreate()

  // Read a normal DF from file
  val tablesDF: DataFrame = sparkSession.read.parquet("data/master_df")
  tablesDF.show()

  import sparkSession.sql

  sql("CREATE DATABASE IF NOT EXISTS DEMO") // to create DB manually, else it uses a default DB called 'default'
  sql("USE DEMO") // For setting current DB for the session OR use fully qualified name while saving DEMO.table_info

  /*
    For creating external Table
    sql(s"CREATE EXTERNAL TABLE ext_tbl(id bigint) STORED AS PARQUET LOCATION 'home/shredder/...'")
   */

  // TO save the DF as managed Table as Parquet(default)
  tablesDF
    .write
    .mode(SaveMode.Overwrite)
    .partitionBy("table_schema") //bucketing / partitioning etc can be enabled too
    .saveAsTable("tables_info") // Save a permanent table , where data is stored at warehouse, catalog metadata at metastore_db

  // The catalog has many functions that gives valuable info
  sparkSession.catalog.listTables().show()

  // TO load previously saved table
  sparkSession.table("tables_info").show()

  sparkSession.close()
}
