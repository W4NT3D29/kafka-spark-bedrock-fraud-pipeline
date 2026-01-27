package com.omarfg.fraud

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{LongType, StringType, StructType}
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.avro.Schema
import org.apache.spark.sql.avro.functions._ // Add this for from_avro

object FraudPipelineApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Kafka Spark Fraud Detection")
      .master("spark://spark-master:7077") // For cluster submit
      .getOrCreate()

    import spark.implicits._

    // Schema Registry config
    val schemaRegistryUrl = "http://schema-registry:8081"
    val subject = "transactions-raw-value"
    val registry = new CachedSchemaRegistryClient(schemaRegistryUrl, 100)
    val avroSchemaStr = registry.getLatestSchemaMetadata(subject).getSchema
    val avroSchema = new Schema.Parser().parse(avroSchemaStr)

    // Define Spark schema from Avro (manual mapping for simplicity)
    val transactionSchema = new StructType()
      .add("transaction_id", StringType)
      .add("user_id", StringType)
      .add("amount", "double")
      .add("currency", StringType)
      .add("merchant", StringType)
      .add("category", StringType, true)
      .add("timestamp", LongType)
      .add("country", StringType)
      .add("device_type", StringType)
      .add("is_fraud", "boolean")

    // Read stream from Kafka
    val rawDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka-1:29092,kafka-2:29092")
      .option("subscribe", "transactions-raw")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()

    // Parse Avro value
    val parsedDf = rawDf
      .select(
        from_avro($"value", avroSchemaStr).as("transaction")
      )
      .select("transaction.*")

    // Convert timestamp millis to TimestampType
    val timedDf = parsedDf.withColumn(
      "timestamp",
      to_timestamp(from_unixtime($"timestamp" / 1000))
    )

    // Add watermark + windowed features
    val featuredDf = timedDf
      .withWatermark("timestamp", "10 minutes")
      .groupBy(
        window($"timestamp", "5 minutes", "1 minute"),
        $"user_id"
      )
      .agg(
        sum($"amount").as("window_amount"),
        count("*").as("window_count"),
        avg($"amount").as("avg_amount")
      )
      .withColumn(
        "velocity",
        $"window_amount" / 5
      ) // Approx amount per min (5-min window)

    // Simple rule-based fraud detection
    val flaggedDf = featuredDf
      .withColumn(
        "fraud_score",
        when(
          $"velocity" > 1000 || $"window_count" > 50 || $"avg_amount" > 2000,
          lit(0.8) // High risk
        ).otherwise(lit(0.1))
      ) // Low risk
      .filter($"fraud_score" > 0.5) // Flag high risk

    // Sink to console (for testing) + new Kafka topic
    val query = flaggedDf.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    // Also sink to Kafka (flagged-transactions)
    flaggedDf.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka-1:29092,kafka-2:29092")
      .option("topic", "flagged-transactions")
      .option("checkpointLocation", "/tmp/spark-checkpoint")
      .start()

    query.awaitTermination()
  }
}
