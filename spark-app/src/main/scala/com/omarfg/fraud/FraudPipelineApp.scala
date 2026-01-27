package com.omarfg.fraud

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{LongType, StringType, StructType}
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.avro.Schema
import org.apache.spark.sql.avro.functions._

object FraudPipelineApp {
  
  // UDF to strip Confluent Schema Registry header (5 bytes: magic byte + 4-byte schema ID)
  val stripSchemaRegistryHeader = udf((payload: Array[Byte]) => {
    if (payload != null && payload.length > 5) {
      payload.drop(5) // Skip first 5 bytes
    } else {
      payload
    }
  })
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Kafka Spark Fraud Detection")
      .master("spark://spark-master:7077")
      .config(
        "spark.streaming.stopGracefullyOnShutdown",
        "true"
      ) // Graceful stop
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("INFO") // More verbose for debug

    val schemaRegistryUrl = "http://schema-registry:8081"
    val subject = "transactions-raw-value"
    val registry = new CachedSchemaRegistryClient(schemaRegistryUrl, 100)
    val avroSchemaStr = registry.getLatestSchemaMetadata(subject).getSchema
    val avroSchema = new Schema.Parser().parse(avroSchemaStr)

    val transactionSchema = new StructType()
      .add("transaction_id", StringType, false)
      .add("user_id", StringType, false)
      .add("amount", "double", false)
      .add("currency", StringType, false)
      .add("merchant", StringType, false)
      .add("category", StringType, true) // Nullable
      .add("timestamp", LongType, false)
      .add("country", StringType, false)
      .add("device_type", StringType, false)
      .add("is_fraud", "boolean", false)

    val rawDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka-1:29092")
      .option("subscribe", "transactions-raw")
      .option("startingOffsets", "earliest") // Read all data for testing
      .option("failOnDataLoss", "false")
      .load()

    // Strip Schema Registry header (5 bytes) and then deserialize Avro
    val parsedDf = rawDf
      .withColumn("avro_payload", stripSchemaRegistryHeader($"value"))
      .select(
        from_avro($"avro_payload", avroSchemaStr).as("transaction")
      )
      .select("transaction.*")

    val timedDf = parsedDf.withColumn(
      "timestamp",
      to_timestamp(from_unixtime($"timestamp" / 1000))
    )

    // ============================================================
    // APPROACH 1: IMMEDIATE RULE-BASED FRAUD DETECTION
    // Simple threshold rules - good for showcase/demos
    // ============================================================
    println("=== Setting up immediate fraud detection ===")
    val immediateFlags = timedDf.withColumn(
      "fraud_reason",
      when($"amount" > 5000, "HIGH_AMOUNT")
        .when($"is_fraud" === true, "LABELED_FRAUD")
        .otherwise("NORMAL")
    ).withColumn(
      "rule_score",
      when($"amount" > 5000, lit(0.9))
        .when($"is_fraud" === true, lit(0.8))
        .otherwise(lit(0.1))
    )

    val flaggedImmediate = immediateFlags
      .filter($"rule_score" > 0.5)
      .select(
        $"transaction_id",
        $"user_id",
        $"amount",
        $"currency",
        $"merchant",
        $"timestamp",
        $"fraud_reason",
        $"rule_score",
        $"is_fraud"
      )

    // ============================================================
    // APPROACH 2: WINDOWED VELOCITY-BASED FRAUD DETECTION
    // Realistic pattern detection - production-grade approach
    // Detects: rapid transactions, high velocity, unusual patterns
    // ============================================================
    println("=== Setting up windowed velocity-based fraud detection ===")
    val featuredDf = timedDf
      .withWatermark("timestamp", "2 minutes")  // Allow 2 min late data
      .groupBy(
        window($"timestamp", "2 minutes", "30 seconds"),  // 2-min window, 30-sec slide
        $"user_id"
      )
      .agg(
        sum($"amount").as("window_amount"),
        count("*").as("window_count"),
        avg($"amount").as("avg_amount"),
        max($"amount").as("max_amount")
      )
      .withColumn("velocity", $"window_amount" / 2)  // Amount per minute

    // Realistic fraud scoring based on velocity and patterns
    val windowedFlags = featuredDf.withColumn(
      "fraud_reason",
      when($"velocity" > 2500, "HIGH_VELOCITY")
        .when($"window_count" > 3, "RAPID_TRANSACTIONS")
        .when($"avg_amount" > 3000, "HIGH_AVERAGE")
        .otherwise("PATTERN_DETECTED")
    ).withColumn(
      "rule_score",
      when($"velocity" > 5000, lit(0.95))  // Very high velocity
        .when($"velocity" > 2500 || $"window_count" > 5, lit(0.85))  // High risk
        .when($"velocity" > 1000 || $"window_count" > 3 || $"avg_amount" > 3000, lit(0.70))  // Medium risk
        .otherwise(lit(0.1))
    ).filter($"rule_score" > 0.5)
      .select(
        $"window",
        $"user_id",
        $"window_amount",
        $"window_count",
        $"avg_amount",
        $"max_amount",
        $"velocity",
        $"fraud_reason",
        $"rule_score"
      )

    // Debug: Print windowed aggregations (all windows, not just flagged)
    println("=== Starting windowed aggregations debug stream ===")
    featuredDf.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .option("numRows", "10")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .queryName("windowed-aggregations-debug")
      .start()

    // IMMEDIATE Flagged transactions console sink
    println("=== Starting IMMEDIATE flagged transactions console stream ===")
    val consoleQuery = flaggedImmediate.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .queryName("flagged-immediate-console")
      .start()

    // IMMEDIATE Kafka sink: Write flagged transactions to Kafka topic
    println("=== Starting IMMEDIATE flagged transactions Kafka stream ===")
    
    // Convert DataFrame to JSON for Kafka
    val kafkaFlaggedDf = flaggedImmediate
      .select(
        $"user_id".cast("string").as("key"),
        to_json(struct($"*")).cast("string").as("value")
      )
    
    kafkaFlaggedDf.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka-1:29092")
      .option("topic", "flagged-transactions")
      .option("checkpointLocation", "/tmp/spark-checkpoint-flagged-immediate")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .queryName("flagged-immediate-kafka")
      .start()

    // WINDOWED Flagged to separate topic (for advanced use case)
    println("=== Starting WINDOWED flagged transactions Kafka stream ===")
    val kafkaWindowedDf = windowedFlags
      .select(
        $"user_id".cast("string").as("key"),
        to_json(struct($"*")).cast("string").as("value")
      )
    
    kafkaWindowedDf.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka-1:29092")
      .option("topic", "flagged-transactions")
      .option("checkpointLocation", "/tmp/spark-checkpoint-flagged-windowed")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .queryName("flagged-windowed-kafka")
      .start()

    println("=== All streams started. Waiting for data... ===")
    println("=== Watch for HIGH_AMOUNT transactions (>$5000) in console ===")
    consoleQuery.awaitTermination() // Keeps job running until manual stop
  }
}
