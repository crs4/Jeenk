name := "bcl-converter"
version := "0.1"

val fver = "1.1-SNAPSHOT"

resolvers += Resolver.mavenLocal
// resolvers += "apache-snapshot" at "https://repository.apache.org/content/repositories/snapshots/"

libraryDependencies ++= Seq(
  // -------------------------------------
  "com.google.guava" % "guava" % "19.0" ,
  "org.apache.hadoop"  % "hadoop-common" % "2.7.2" ,
  "org.apache.hadoop"  % "hadoop-dist" % "2.7.2" ,
  "org.apache.parquet" % "parquet-avro" % "1.8.1" ,
  "org.apache.flink" %% "flink-scala" % fver ,
  "org.apache.flink" %% "flink-clients" % fver ,
  "org.apache.flink" %% "flink-streaming-scala" % fver
  // -------------------------------------
  // "io.netty" % "netty" % "3.9.8.Final" force()
  // "" % "" % "",
  // "" % "" % "",
)


fork in run := true

