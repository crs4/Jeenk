name := "bcl converter"
version := "0.1"

val fver = "1.1-SNAPSHOT"

resolvers += Resolver.mavenLocal
// resolvers += "apache-snapshot" at "https://repository.apache.org/content/repositories/snapshots/"

libraryDependencies ++= Seq(
  // -------------------------------------
  "org.apache.spark" %% "spark-core"   % "1.4.0" ,
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

