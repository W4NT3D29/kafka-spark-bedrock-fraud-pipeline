#!/bin/bash

# Script to build Scala Spark app and submit to local Docker cluster
# Usage: ./scripts/build-and-submit.sh [class_name] [jar_name]
# Defaults: com.omarfg.fraud.FraudPipelineApp kafka-spark-fraud-pipeline-assembly-0.1.0-SNAPSHOT.jar

# Set defaults
CLASS_NAME=${1:-com.omarfg.fraud.FraudPipelineApp}
JAR_NAME=${2:-kafka-spark-fraud-pipeline-assembly-0.1.0-SNAPSHOT.jar}

# Paths
PROJECT_ROOT=$(pwd)
SPARK_APP_DIR="$PROJECT_ROOT/spark-app"
JAR_PATH="$SPARK_APP_DIR/$JAR_NAME"  # Assumes root of spark-app/ after custom assemblyOutputPath

# Step 1: Build the JAR
echo "Building JAR with sbt..."
cd "$SPARK_APP_DIR" || { echo "Error: spark-app dir not found"; exit 1; }
sbt clean assembly || { echo "Error: sbt assembly failed"; exit 1; }

# Check if JAR exists on host
if [ ! -f "$JAR_PATH" ]; then
  echo "Error: JAR not found at $JAR_PATH"
  exit 1
fi

# Step 2: Verify Docker stack is running and mount exists
echo "Verifying Docker stack..."
docker ps | grep spark-master > /dev/null || { echo "Error: spark-master not running. Run docker-compose up -d first."; exit 1; }

# Check JAR visible in container
docker exec spark-master ls /opt/spark/app/"$JAR_NAME" > /dev/null 2>&1 || { echo "Error: JAR not visible in container at /opt/spark/app/$JAR_NAME"; exit 1; }
echo "JAR verified in container."

# Step 3: Submit the job
echo "Submitting job to Spark cluster..."
docker exec -it spark-master \
  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --class "$CLASS_NAME" \
    --deploy-mode client \
    --conf spark.driver.extraJavaOptions="-Djava.security.manager=allow" \
    --conf spark.executor.extraJavaOptions="-Djava.security.manager=allow" \
    /opt/spark/app/"$JAR_NAME"

echo "Job submission complete. Check Spark UI at http://localhost:8082 for details."
