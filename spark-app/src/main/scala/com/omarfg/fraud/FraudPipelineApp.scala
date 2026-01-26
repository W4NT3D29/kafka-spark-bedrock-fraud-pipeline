package com.omarfg.fraud

import org.apache.spark.sql.SparkSession

object FraudPipelineApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Fraud Detection Pipeline - Local Test")
      .getOrCreate()

    println("Spark session created successfully!")
    println(s"Spark version: ${spark.version}")

    // Just to confirm config
    spark.sparkContext.uiWebUrl.foreach(println)

    spark.stop()
  }
}