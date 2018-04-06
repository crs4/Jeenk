name := "bcl-converter"
version := "0.1"
scalaVersion := "2.11.11" // "2.10.5"
scalacOptions ++= Seq("unchecked", "-deprecation", "-feature")

val fver = "1.4.2" //"1.3.2" //"1.5-SNAPSHOT" 

// resolvers += Resolver.mavenLocal
// resolvers += "apache-snapshot" at "https://repository.apache.org/content/repositories/snapshots/"
// resolvers += "apache-snapshot" at "https://repository.apache.org/content/repositories/releases/"

libraryDependencies ++= Seq(
  // -------------------------------------
  // "" % "" % "",
  // "junit" % "junit" % "4.12" ,
  "com.google.guava" % "guava" % "19.0" ,
  "org.seqdoop" % "hadoop-bam" % "7.8.0" ,
  "com.github.samtools" % "htsjdk" % "2.13.1" ,
  "org.apache.parquet" % "parquet-avro" % "1.8.1" ,
  "org.apache.flink" %% "flink-streaming-scala" % fver % "provided" ,
  "org.apache.flink" %% "flink-hadoop-compatibility" % fver % "provided" ,
  "org.apache.flink" %% "flink-connector-kafka-0.11" % fver ,
  "org.apache.flink" %% "flink-connector-filesystem" % fver
)

excludeDependencies ++= Seq(
  // ExclusionRule("", "") ,
  ExclusionRule("org.apache.hadoop", "hadoop-yarn-api") ,
  ExclusionRule("org.apache.hadoop", "hadoop-yarn-common") ,
  ExclusionRule("org.apache.hadoop", "hadoop-yarn-client") ,
  ExclusionRule("org.apache.hadoop", "hadoop-common") ,
  ExclusionRule("org.apache.hadoop", "hadoop-hdfs") ,
  ExclusionRule("org.apache.hadoop", "hadoop-annotations") ,
  ExclusionRule("org.apache.hadoop", "hadoop-mapreduce-client-core") ,
  ExclusionRule("stax", "stax-api")
)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

fork in run := true
