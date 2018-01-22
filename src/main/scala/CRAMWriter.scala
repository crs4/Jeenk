package bclconverter.writer

import com.typesafe.config.ConfigFactory
import htsjdk.samtools.SAMRecord
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
import org.apache.flink.streaming.api.scala.extensions._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers._
import org.apache.flink.streaming.api.windowing.windows.{Window, TimeWindow, GlobalWindow}
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue
import org.apache.flink.streaming.util.serialization.{KeyedSerializationSchema, KeyedDeserializationSchema, DeserializationSchema}
import org.apache.flink.util.MathUtils
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._
import scala.collection.mutable.Map
import scala.concurrent.{ExecutionContext, Await, Future}

import bclconverter.kafka.{MySSerializer, MyPartitioner, ProdProps, ConsProps, MyDeserializer, MySDeserializer}
import bclconverter.reader.Reader.{Block}
import bclconverter.aligner.MyWaterMarker


class WList(val param : ParameterTool) extends Serializable{
  // parameters
  val root = param.getRequired("root")
  val fout = param.getRequired("fout")
  val rapiwin = param.getInt("rapiwin", 1024)
  val flinkpar = param.getInt("writerflinkpar", 1)
  val crampar = param.getInt("crampar", flinkpar)
  val kafkapar = param.getInt("kafkapar", 1)
  val rapipar = param.getInt("rapipar", 1)
  val wgrouping = param.getInt("wgrouping", 1)
  val kafkaServer = param.get("kafkaServer", "127.0.0.1:9092")
  val kafkaAligned = param.get("kafkaAligned", "flink-aligned")
  val kafkaControl = param.get("kafkaControl", "flink-con")
  val stateBE = param.getRequired("stateBE")
  val sref = param.getRequired("reference")
}

class miniWriter(pl : WList, ind : (Int, Int)) {
  // initialize stream environment
  var env = StreamExecutionEnvironment.getExecutionEnvironment
  env.getConfig.setGlobalJobParameters(pl.param)
  env.setParallelism(pl.flinkpar)
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  // env.enableCheckpointing(30000)
  // env.getCheckpointConfig.setMinPauseBetweenCheckpoints(15000)
  // env.getCheckpointConfig.setCheckpointTimeout(10000)
  // env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
  env.setStateBackend(new FsStateBackend(pl.stateBE, true))
  var jobs = List[(Int, String, String)]()
  def writeToOF(x : (DataStream[SAMRecord], String)) = {
    val wr = new bclconverter.sam.CRAMWriter(pl.sref)
    val fname = pl.fout + x._2 + ".cram"
    val bucket = new BucketingSink[SAMRecord](fname)
      .setWriter(wr)
      .setBatchSize(1024 * 1024 * 8)
      .setInactiveBucketCheckInterval(10000)
      .setInactiveBucketThreshold(10000)
    x._1
      .addSink(bucket)
      .setParallelism(pl.crampar)
      .name(fname)
  }
  def add(id : Int, topicname : String, filename : String) {
    jobs ::= (id, topicname, filename)
  }
  def doJob(id : Int, topicname : String, filename : String) = {
    val props = new ConsProps("outconsumer11.", pl.kafkaServer)
    props.put("auto.offset.reset", "earliest")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "10000")
    val cons = new FlinkKafkaConsumer011[SAMRecord](topicname, new MySDeserializer(pl.flinkpar), props)
    val sam = env
      .addSource(cons)
      .setParallelism(pl.kafkapar)
      .assignTimestampsAndWatermarks(new MyWaterMarker[SAMRecord])
      .name(topicname)

    writeToOF(sam, filename)
  }
  def go = {
    jobs.foreach(j => doJob(j._1, j._2, j._3))
    env.execute(s"CRAM Writer ${ind._1}/${ind._2}")
  }
}

class Writer(pl : WList) {
  def kafka2cram(toc : Array[String]) : Iterable[miniWriter] = {
    val filenames = toc
      .map(s => s.split(" ", 2))
      .map(r => (r.head, r.last)).toMap
    def RW(ids : Iterable[Int], ind : (Int, Int)) : miniWriter = {
      val mw = new miniWriter(pl, ind)
      ids.foreach{
        id =>
        val topicname = pl.kafkaAligned + "-" + id.toString
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

    val pl = new WList(params)
    val rg = new scala.util.Random
    val cp = new ConsProps("conconsumer11.", pl.kafkaServer)
    cp.put("auto.offset.reset", "earliest")
    cp.put("enable.auto.commit", "true")
    cp.put("auto.commit.interval.ms", "10000")
    val conConsumer = new KafkaConsumer[Void, String](cp)
    val rw = new Writer(pl)
    conConsumer.subscribe(List(pl.kafkaControl))
    var jobs = List[Future[Any]]()
    while (true) {
      val records = conConsumer.poll(3000)
      // records.foreach(_ => println("Adding job..."))
      jobs ++= records
        .filter(_.key == 1)
        .flatMap
      {
	r =>
	  rw.kafka2cram(r.value.split("\n"))
	    .map(j => Future{j.go})
      }
    }
    conConsumer.close
  }
}
