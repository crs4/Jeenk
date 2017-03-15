name := "bcl-converter"
version := "0.1"
scalaVersion := "2.10.5" //"2.11.8"

val fver = "1.1.3" //"1.2.0" //"1.3-SNAPSHOT" 

resolvers += Resolver.mavenLocal
// resolvers += "apache-snapshot" at "https://repository.apache.org/content/repositories/snapshots/"

libraryDependencies ++= Seq(
  // -------------------------------------
  // "io.netty" % "netty" % "3.9.8.Final" force()
  // "" % "" % "",
  // "" % "" % "",
  // "junit" % "junit" % "4.12" ,
  "com.google.guava" % "guava" % "19.0" ,
  "org.seqdoop" % "hadoop-bam" % "7.8.0" ,
  // "org.apache.hadoop"  % "hadoop-common" % "2.7.2-mia" ,
  // "org.apache.flink" % "flink-shaded-hadoop2" % fver , 
  "org.apache.parquet" % "parquet-avro" % "1.8.1" ,
  "org.apache.flink" %% "flink-hadoop-compatibility" % fver ,
  "org.apache.flink" %% "flink-scala" % fver ,
  "org.apache.flink" %% "flink-clients" % fver ,
  "org.apache.flink" %% "flink-streaming-scala" % fver
)

excludeDependencies ++= Seq(
  // SbtExclusionRule("", "") ,
  // SbtExclusionRule("org.codehaus.jackson", "*") ,
  SbtExclusionRule("org.apache.hadoop", "hadoop-yarn-api") ,
  SbtExclusionRule("org.apache.flink", "flink-shaded-hadoop2") ,
  SbtExclusionRule("commons-beanutils", "commons-beanutils") ,
  SbtExclusionRule("commons-beanutils", "commons-beanutils-core") 
  // SbtExclusionRule("com.google.code.findbugs", "*")
  // SbtExclusionRule("net.java.dev.jets3t", "*")
)

fork in run := true
