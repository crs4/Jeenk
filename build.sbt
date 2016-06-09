name := "bcl-converter"
version := "0.1"

val fver = "1.0.3" //"1.1-SNAPSHOT"

resolvers += Resolver.mavenLocal
// resolvers += "apache-snapshot" at "https://repository.apache.org/content/repositories/snapshots/"

libraryDependencies ++= Seq(
  // -------------------------------------
  // "io.netty" % "netty" % "3.9.8.Final" force()
  // "" % "" % "",
  // "" % "" % "",
  "com.google.guava" % "guava" % "19.0" ,
  // "org.apache.hadoop"  % "hadoop-common" % "2.7.2-mia" ,
  "org.apache.parquet" % "parquet-avro" % "1.8.1" ,
  "org.apache.flink" %% "flink-scala" % fver ,// % "provided" ,
  "org.apache.flink" %% "flink-clients" % fver ,// % "provided" ,
  "org.apache.flink" %% "flink-streaming-scala" % fver // % "provided"
)

excludeDependencies ++= Seq(
  // SbtExclusionRule("", "") ,
  // SbtExclusionRule("commons-beanutils", "*") ,
  // SbtExclusionRule("org.apache.flink", "flink-shaded-hadoop2") ,
  // SbtExclusionRule("net.java.dev.jets3t", "*")
)

fork in run := true

