package bclconverter.aligner

import com.typesafe.config.ConfigFactory
import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.Executors
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.hadoop.mapreduce.HadoopOutputFormat
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
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat => MapreduceFileOutputFormat, NullOutputFormat}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.seqdoop.hadoop_bam.SAMRecordWritable
import scala.collection.JavaConversions._
import scala.concurrent.{ExecutionContext, Await, Future}

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

class MyDeserializer extends DeserializationSchema[(Int, PRQData)] {
  var eos = false
  var key = 0
  val keyspace = 64
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

class ConsProps(pref: String) extends Properties {
  private val pkeys = Seq("bootstrap.servers", "group.id", "request.timeout.ms",
  "value.deserializer", "key.deserializer", "auto.offset.reset").map(pref + _)
  lazy val typesafeConfig = ConfigFactory.load()

  pkeys.map{ key =>
    if (typesafeConfig.hasPath(key)) {
      put(key.replace(pref, ""), typesafeConfig.getString(key))
    }
  }
  put("client.id", "robo" + ConsProps.rg.nextLong.toString)

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
  val kafkaTopic = param.get("kafkaTopic", "flink-prq")
  val kafkaControl = param.get("kafkaControl", "flink-con")
  val stateBE = param.getRequired("stateBE")
  val sref = param.getRequired("reference")
  val header = param.getRequired("header")
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

class miniWriter(pl : PList) {
  // initialize stream environment
  var env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(pl.flinkpar)
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.enableCheckpointing(10000)
  env.setStateBackend(new FsStateBackend(pl.stateBE, true))
  var jobs = List[(Int, String, String)]()
  var hofs = List[HadoopOutputFormat[LongWritable, SAMRecordWritable]]()
  def writeToOF(x : (DataStream[SAMRecordWritable], String)) = {
    val s2c = new SAM2CRAM(pl.header, "file://" + pl.sref)
    val opath = new HPath(x._2 + ".cram")
    val job = Job.getInstance(new HConf)
    MapreduceFileOutputFormat.setOutputPath(job, opath)
    val hof = new MyCRAMOutputFormat(s2c, job)
    hofs ::= hof
    // val hof = new HadoopOutputFormat(new NullOutputFormat[LongWritable, SAMRecordWritable], job)
    // write to cram
    x._1
      .map(s => (new LongWritable(0), s))
      .writeUsingOutputFormat(hof)
  }
  def add(id : Int, topicname : String, filename : String) {
    jobs ::= (id, topicname, filename)
  }
  def finalizeAll = {
    hofs.foreach(hof => hof.finalizeGlobal(1))
  }
  def doJob(id : Int, topicname : String, filename : String) = {
    val props = new ConsProps("outconsumer10.")
    props.put("auto.offset.reset", "earliest")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "10000")
    val cons = new FlinkKafkaConsumer010[(Int, PRQData)](topicname, new MyDeserializer, props)
      .assignTimestampsAndWatermarks(new MyWaterMarker[(Int, PRQData)])
    val ds = env
      .addSource(cons)
      .setParallelism(pl.kafkapar)
    val sam = ds
      .keyBy(0)
      .timeWindow(Time.milliseconds(pl.rapiwin))
      .apply(new PRQ2SAMRecord[TimeWindow](pl.sref, pl.rapipar))

    writeToOF(sam, filename)
  }
  def go = {
    jobs.foreach(j => doJob(j._1, j._2, j._3))
    env.execute
    finalizeAll
  }
}

class Writer(pl : PList) {
  def kafka2cram(toc : Array[String]) : Iterable[miniWriter] = {
    val filenames = toc
      .map(s => s.split(" ", 2))
      .map(r => (r.head, r.last)).toMap
    def RW(ids : Iterable[Int]) : miniWriter = {
      val mw = new miniWriter(pl)
      ids.foreach{
        id =>
        val topicname = pl.kafkaTopic + "-" + id.toString
        mw.add(id, topicname, filenames(id.toString))
      }
      mw
    }
    // start here
    val ids = filenames.keys.map(_.toInt)
    ids.grouped(pl.wgrouping).map(lid => RW(lid)).toIterable
  }
}

object runWriter {
  def main(args: Array[String]) {
    val propertiesFile = "conf/bclconverter.properties"
    val param = ParameterTool.fromPropertiesFile(propertiesFile)
    val numTasks = param.getInt("numWriters") // concurrent flink tasks to be run
    implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(numTasks))
    // implicit val timeout = Timeout(30 seconds)

    val rg = new scala.util.Random
    val cp = new ConsProps("conconsumer10.")
    cp.put("auto.offset.reset", "earliest")
    cp.put("enable.auto.commit", "true")
    cp.put("auto.commit.interval.ms", "1000")
    val conConsumer = new KafkaConsumer[Void, String](cp)
    val pl = new PList(param)
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
