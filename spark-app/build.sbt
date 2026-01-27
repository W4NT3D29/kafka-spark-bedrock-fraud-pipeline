ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18" // Matches Spark 3.5.x default

lazy val root = (project in file("."))
  .settings(
    name := "kafka-spark-fraud-pipeline",

    // Spark dependencies (provided scope since runtime from Docker image)
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.3" % Provided,
      "org.apache.spark" %% "spark-sql" % "3.5.3" % Provided,
      "org.apache.spark" %% "spark-streaming" % "3.5.3" % Provided,
      // Include Kafka and Avro in assembly JAR (not Provided)
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.3",
      "org.apache.spark" %% "spark-avro" % "3.5.3",
      "com.twitter" %% "bijection-avro" % "0.9.7",
      "io.confluent" % "kafka-avro-serializer" % "7.7.0",
      "io.confluent" % "kafka-schema-registry-client" % "7.7.0",
      "org.slf4j" % "slf4j-api" % "2.0.16",
      "org.apache.logging.log4j" % "log4j-slf4j2-impl" % "2.23.1" % Runtime
    ),

    // Confluent repo for schema registry deps
    resolvers += "Confluent" at "https://packages.confluent.io/maven/",

    // Assembly settings (to create fat JAR with all dependencies)
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", xs @ _*) => MergeStrategy.concat
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case "module-info.class" => MergeStrategy.discard
      case x if x.endsWith(".proto") => MergeStrategy.first
      case x if x.contains("commons-logging") => MergeStrategy.first
      case x => MergeStrategy.first
    },
    assembly / assemblyOutputPath := baseDirectory.value / s"${name.value}-assembly-${version.value}.jar"
  )
