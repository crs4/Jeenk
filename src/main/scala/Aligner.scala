package bclconverter.aligner

import com.typesafe.config.ConfigFactory
import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.Executors
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{Window, TimeWindow, GlobalWindow}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue
import org.apache.flink.streaming.util.serialization.{KeyedSerializationSchema, KeyedDeserializationSchema, DeserializationSchema}
import org.apache.hadoop.conf.{Configuration => HConf}
import org.apache.hadoop.fs.{FileSystem, FSDataInputStream, FSDataOutputStream, Path => HPath}
import org.apache.hadoop.io.{NullWritable, LongWritable}
import org.apache.hadoop.mapreduce.Job
import org.apache.kafka.clients.consumer.KafkaConsumer
// import org.seqdoop.hadoop_bam.SAMRecordWritable
import htsjdk.samtools.SAMRecord
import scala.collection.JavaConversions._
import scala.concurrent.{ExecutionContext, Await, Future}
import org.apache.flink.streaming.api.windowing.triggers._
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink

import bclconverter.reader.Reader.{Block, PRQData}

class MyWaterMarker[T] extends AssignerWithPeriodicWatermarks[T] {
  var cur = 0l
  override def extractTimestamp(el: T, prev: Long): Long = {
    val r = cur
    cur += 1
    return r
  }
  override def getCurrentWatermark : Watermark = {
    new Watermark(cur - 1)
  }
}

class MyDeserializer(fpar : Int) extends DeserializationSchema[(Int, PRQData)] {
  var eos = false
  var key = 0
  var keyspace = 8 * fpar
  override def getProducedType = TypeInformation.of(classOf[(Int, PRQData)])
  override def isEndOfStream(el : (Int, PRQData)) : Boolean = {
    if (el._2._1.size > 1)
      false
    else
      true
  }
  override def deserialize(data : Array[Byte]) : (Int, PRQData) = {
    val rkey = key
    key = (key + 1) % keyspace
    val (s1, r1) = data.splitAt(4)
    val (p1, d1) = r1.splitAt(toInt(s1))
    val (s2, r2) = d1.splitAt(4)
    val (p2, d2) = r2.splitAt(toInt(s2))
    val (s3, r3) = d2.splitAt(4)
    val (p3, d3) = r3.splitAt(toInt(s3))
    val (s4, r4) = d3.splitAt(4)
    val (p4, d4) = r4.splitAt(toInt(s4))
    val (s5, p5) = d4.splitAt(4)
    (rkey, (p1, p2, p3, p4, p5))
  }
  def toInt(a : Array[Byte]) : Int = {
    ByteBuffer.wrap(a).getInt
  }
}

object ConsProps {
  val rg = new scala.util.Random
}

class ConsProps(pref: String, kafkaServer : String) extends Properties {
  private val pkeys = Seq("bootstrap.servers", "group.id", "request.timeout.ms",
  "value.deserializer", "key.deserializer", "auto.offset.reset").map(pref + _)
  lazy val typesafeConfig = ConfigFactory.load()

  pkeys.map{ key =>
    if (typesafeConfig.hasPath(key)) {
      put(key.replace(pref, ""), typesafeConfig.getString(key))
    }
  }
  put("client.id", "robo" + ConsProps.rg.nextLong.toString)
  put("group.id", "robo" + ConsProps.rg.nextLong.toString)
  put("bootstrap.servers", kafkaServer)

  def getCustomString(key: String) = typesafeConfig.getString(key)
  def getCustomInt(key: String) = typesafeConfig.getInt(key)
}

class PList(param : ParameterTool) extends Serializable{
  // parameters
  val root = param.getRequired("root")
  val fout = param.getRequired("fout")
  val rapiwin = param.getInt("rapiwin", 1024)
  val flinkpar = param.getInt("writerflinkpar", 1)
  val kafkapar = param.getInt("kafkapar", 1)
  val rapipar = param.getInt("rapipar", 1)
  val wgrouping = param.getInt("wgrouping", 1)
  val kafkaServer = param.get("kafkaServer", "127.0.0.1:9092")
  val kafkaTopic = param.get("kafkaTopic", "flink-prq")
  val kafkaControl = param.get("kafkaControl", "flink-con")
  val stateBE = param.getRequired("stateBE")
  val sref = param.getRequired("reference")
}

object Writer {
  def MyFS(path : HPath = null) : FileSystem = {
    var fs : FileSystem = null
    val conf = new HConf
    if (path == null)
      fs = FileSystem.get(conf)
    else {
      fs = FileSystem.get(path.toUri, conf);
    }
    // return the filesystem
    fs
  }
}

class miniWriter(pl : PList, ind : (Int, Int)) {
  // initialize stream environment
  var env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(pl.flinkpar)
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  // env.enableCheckpointing(60000)
  // env.getCheckpointConfig.setMinPauseBetweenCheckpoints(30000)
  // env.getCheckpointConfig.setCheckpointTimeout(20000)
  // env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
  // env.setStateBackend(new FsStateBackend(pl.stateBE, true))
  var jobs = List[(Int, String, String)]()
  def writeToOF(x : (DataStream[SAMRecord], String)) = {
    val wr = new CRAMWriter(pl.sref)
    val fname = x._2 + ".cram"
    val bucket = new BucketingSink[SAMRecord](fname)
      .setWriter(wr)
      .setBatchSize(1024 * 1024 * 8)
      .setInactiveBucketCheckInterval(10000)
      .setInactiveBucketThreshold(10000)
    x._1
      .addSink(bucket)
      .name(fname)
  }
  def add(id : Int, topicname : String, filename : String) {
    jobs ::= (id, topicname, filename)
  }
  def doJob(id : Int, topicname : String, filename : String) = {
    val props = new ConsProps("outconsumer10.", pl.kafkaServer)
    props.put("auto.offset.reset", "earliest")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "10000")
    val cons = new FlinkKafkaConsumer010[(Int, PRQData)](topicname, new MyDeserializer(pl.flinkpar), props)
      .assignTimestampsAndWatermarks(new MyWaterMarker[(Int, PRQData)])
    val ds = env
      .addSource(cons)
      .name(topicname)
      .setParallelism(pl.kafkapar)
    val sam = ds
      .keyBy(0)
      .timeWindow(Time.milliseconds(pl.flinkpar * pl.rapiwin * 2 / 3))
      //.countWindow(pl.rapiwin)
      .apply(new PRQAligner[TimeWindow](pl.sref, pl.rapipar))

    writeToOF(sam, filename)
  }
  def go = {
    jobs.foreach(j => doJob(j._1, j._2, j._3))
    env.execute(s"Aligner ${ind._1}/${ind._2}")
  }
}

class Writer(pl : PList) {
  def kafka2cram(toc : Array[String]) : Iterable[miniWriter] = {
    val filenames = toc
      .map(s => s.split(" ", 2))
      .map(r => (r.head, r.last)).toMap
    def RW(ids : Iterable[Int], ind : (Int, Int)) : miniWriter = {
      val mw = new miniWriter(pl, ind)
      ids.foreach{
        id =>
        val topicname = pl.kafkaTopic + "-" + id.toString
        mw.add(id, topicname, filenames(id.toString))
      }
      mw
    }
    val ids = filenames.keys.map(_.toInt)
    val g = ids.grouped(pl.wgrouping).toArray
    val n = g.size
    g.indices.map(i => RW(g(i), (i+1, n)))
  }
}

object runWriter {
  def main(args: Array[String]) {
    val pargs = ParameterTool.fromArgs(args)
    val propertiesFile = pargs.getRequired("properties")
    val pfile = ParameterTool.fromPropertiesFile(propertiesFile)
    val params = pfile.mergeWith(pargs)

    val numTasks = params.getInt("numWriters") // concurrent flink tasks to be run
    implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(numTasks))
    // implicit val timeout = Timeout(30 seconds)

    val pl = new PList(params)
    val rg = new scala.util.Random
    val cp = new ConsProps("conconsumer10.", pl.kafkaServer)
    cp.put("auto.offset.reset", "earliest")
    cp.put("enable.auto.commit", "true")
    cp.put("auto.commit.interval.ms", "10000")
    val conConsumer = new KafkaConsumer[Void, String](cp)
    val rw = new Writer(pl)
    conConsumer.subscribe(List(pl.kafkaControl))
    var jobs = List[Future[Any]]()
    while (true) {
      val records = conConsumer.poll(3000)
      records.foreach(_ => println("Adding job..."))
      jobs ++= records.flatMap
      {
	r =>
	  rw.kafka2cram(r.value.split("\n"))
	    .map(j => Future{j.go})
      }
    }
    conConsumer.close
  }
}
