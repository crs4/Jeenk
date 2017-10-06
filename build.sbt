name := "bcl-converter"
version := "0.1"
scalaVersion := "2.10.5" //"2.11.8"

val fver = "1.3.2" //"1.2.1" //"1.4-SNAPSHOT" 

// resolvers += Resolver.mavenLocal
// resolvers += "apache-snapshot" at "https://repository.apache.org/content/repositories/snapshots/"

libraryDependencies ++= Seq(
  // -------------------------------------
  // "io.netty" % "netty" % "3.9.8.Final" force()
  // "" % "" % "",
  // "" % "" % "",
  // "junit" % "junit" % "4.12" ,
  "com.google.guava" % "guava" % "19.0" ,
  "org.seqdoop" % "hadoop-bam" % "7.8.0" ,
  // "org.apache.hadoop"  % "hadoop-common" % "2.7.3" ,
  "org.apache.parquet" % "parquet-avro" % "1.8.1" ,
  "org.apache.flink" %% "flink-streaming-scala" % fver ,
  "org.apache.flink" %% "flink-hadoop-compatibility" % fver force(),
  "org.apache.flink" %% "flink-connector-kafka-0.10" % fver ,
  "org.apache.flink" %% "flink-connector-filesystem" % fver ,
  "org.apache.flink" %% "flink-clients" % fver
)

excludeDependencies ++= Seq(
  // SbtExclusionRule("", "") ,
  // SbtExclusionRule("org.apache.flink", "flink-shaded-hadoop2") ,
  SbtExclusionRule("org.apache.hadoop", "hadoop-yarn-api") ,
  SbtExclusionRule("org.apache.hadoop", "hadoop-yarn-common") ,
  SbtExclusionRule("org.apache.hadoop", "hadoop-yarn-client") ,
  SbtExclusionRule("org.apache.hadoop", "hadoop-common") ,
  SbtExclusionRule("org.apache.hadoop", "hadoop-hdfs") ,
  SbtExclusionRule("org.apache.hadoop", "hadoop-annotations") ,
  SbtExclusionRule("org.apache.hadoop", "hadoop-mapreduce-client-core") ,
  SbtExclusionRule("stax", "stax-api")
  // SbtExclusionRule("commons-beanutils", "commons-beanutils") ,
  // SbtExclusionRule("commons-beanutils", "commons-beanutils-core") 
)

// assemblyMergeStrategy in assembly := {
//   case "org/apache/flink/api/java/typeutils/TypeExtractor.class" => MergeStrategy.first
//   case x =>
//     val oldStrategy = (assemblyMergeStrategy in assembly).value
//     oldStrategy(x)
// }

fork in run := true
