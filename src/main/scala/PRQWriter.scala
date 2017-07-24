package bclconverter

import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import cz.adamh.utils.NativeUtils
import java.io.OutputStream
import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.Executors
import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.hadoop.mapreduce.HadoopOutputFormat
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows, GlobalWindows}
import org.apache.flink.streaming.api.windowing.evictors.Evictor
import org.apache.flink.streaming.api.windowing.evictors.Evictor.EvictorContext
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{TriggerResult, CountTrigger, PurgingTrigger, Trigger, EventTimeTrigger}
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.windows.{Window, TimeWindow, GlobalWindow}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue
import org.apache.flink.streaming.util.serialization.{KeyedSerializationSchema, KeyedDeserializationSchema, DeserializationSchema}
import org.apache.hadoop.conf.{Configuration => HConf}
import org.apache.hadoop.fs.{FileSystem, FSDataInputStream, FSDataOutputStream, Path => HPath}
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.io.compress.zlib.{ZlibCompressor, ZlibFactory}
import org.apache.hadoop.io.{NullWritable, LongWritable}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat => MapreduceFileOutputFormat}
import org.apache.hadoop.mapreduce.{Job, RecordWriter, TaskAttemptContext}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.seqdoop.hadoop_bam.{AnySAMInputFormat, CRAMInputFormat, SAMRecordWritable, KeyIgnoringCRAMOutputFormat, KeyIgnoringCRAMRecordWriter, KeyIgnoringBAMOutputFormat, KeyIgnoringBAMRecordWriter}
import scala.collection.JavaConversions._
import scala.collection.parallel._
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Await, Future}
import scala.math.max

import java.io.{File, PrintWriter}

import bclconverter.reader.Reader.{Block, PRQData}

class MyTrigger[W <: Window] extends Trigger[PRQData, W] {
  override def clear(win: W, ctx: TriggerContext): Unit = {
  }
  override def onElement(el: PRQData, timestamp: Long, win: W, ctx: TriggerContext): TriggerResult = {
    if (el._1.size > 1) // (timestamp != Long.MaxValue)
      TriggerResult.CONTINUE
    else
      TriggerResult.FIRE_AND_PURGE
  }
  override def onEventTime(x$1: Long,x$2: W,x$3: TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }
  override def onProcessingTime(x$1: Long,x$2: W,x$3: TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }
}

class MyWaterMarker extends AssignerWithPeriodicWatermarks[PRQData] {
  val out = 1000l
  var cur = 0l
  override def extractTimestamp(el: PRQData, prev: Long): Long = {
    cur = prev
    if (el._1.size > 1)
      return prev
    else
      return Long.MaxValue
  }
  override def getCurrentWatermark : Watermark = {
    new Watermark(cur - out)
  }
}

class MyEvictor[W <: Window] extends Evictor[PRQData, W] {
  override
  def evictBefore(els : java.lang.Iterable[TimestampedValue[PRQData]], size : Int, win : W, ec : EvictorContext) = {
    var it = els.iterator
    while (it.hasNext) {
      val e = it.next.getValue._1.size
      if (e == 1)
        it.remove
    }    
  }
  override
  def evictAfter(els : java.lang.Iterable[TimestampedValue[PRQData]], size : Int, win : W, ec : EvictorContext) = {}
}

class MyDeserializer extends DeserializationSchema[PRQData] {
  var eos = false
  override def getProducedType = TypeInformation.of(classOf[PRQData])
  override def isEndOfStream(el : PRQData) : Boolean = {
    //*
    if (el._1.size > 1)
      false
    else
      true
    //*/
    /*
    if (el._1.size > 1)
      false
    else if (eos)
      true
    else {
      eos = true
      false
    }
    */
  }
  override def deserialize(data : Array[Byte]) : PRQData = {
    val (s1, r1) = data.splitAt(4)
    val (p1, d1) = r1.splitAt(toInt(s1))
    val (s2, r2) = d1.splitAt(4)
    val (p2, d2) = r2.splitAt(toInt(s2))
    val (s3, r3) = d2.splitAt(4)
    val (p3, d3) = r3.splitAt(toInt(s3))
    val (s4, r4) = d3.splitAt(4)
    val (p4, d4) = r4.splitAt(toInt(s4))
    val (s5, p5) = d4.splitAt(4)
    (p1, p2, p3, p4, p5)
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

class WData(param : ParameterTool) extends Serializable{
  // parameters
  var kafkaTopic : String = "prq"
  var kafkaControl : String = "kcon"
  var flinkpar = 1
  val root = param.getRequired("root")
  val fout = param.getRequired("fout")
  flinkpar = param.getInt("writerflinkpar", flinkpar)
  kafkaTopic = param.get("kafkaTopic", kafkaTopic)
  kafkaControl = param.get("kafkaControl", kafkaControl)
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

class Writer(wd : WData) extends Serializable{
  // process tile, CRAM output, PRQ as intermediate format
  def kafka2cram(jid : Int, toc : Array[String]) = {
    val FP = StreamExecutionEnvironment.getExecutionEnvironment
    FP.setParallelism(wd.flinkpar)
    FP.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // FP.enableCheckpointing(10000)
    // val maxstate = 256*1024*1024
    // FP.setStateBackend(new MemoryStateBackend(maxstate, true))
    FP.setStateBackend(new FsStateBackend("file:///u/cesco/els/tmp/flink-state-backend", true))
    var ofs = List[HadoopOutputFormat[NullWritable, SAMRecordWritable]]()
    def writeToOF(x : (DataStream[SAMRecordWritable], String)) = {
      val opath = new HPath(x._2)
      val job = Job.getInstance(new HConf)
      MapreduceFileOutputFormat.setOutputPath(job, opath)
      val hof = new HadoopOutputFormat(new SAM2CRAM, job)
      val nw : NullWritable = null
      ofs ::= hof
      // write to cram
      x._1
        .map(s => (nw, s))
        .setParallelism(1)
        .writeUsingOutputFormat(hof)
        .setParallelism(1)
    }
    def writeCount(x : (DataStream[SAMRecordWritable], String)) = {
      val filename = x._2.replaceAll("[/:]","_")
      val ds = x._1
      ds
        .map(_ => 1)
        .writeAsText(s"/u/cesco/els/data/out/count/${filename}.txt")
        .setParallelism(1)
    }
    def writeCount2(x : (DataStream[PRQData], String)) = {
      val filename = x._2.replaceAll("[/:]","_")
      val ds = x._1
      ds
        .map(_ => 1)
        .writeAsText(s"/u/cesco/els/data/out/count/${filename}.txt")
        .setParallelism(1)
    }
    def readFromKafka(ids : Array[Int]) : Array[(Int, DataStream[PRQData])] = {
      var r = Array[(Int, DataStream[PRQData])]()
      for (id <- ids){
        val props = new ConsProps("outconsumer10.")
        props.put("auto.offset.reset", "earliest")
        props.put("enable.auto.commit", "true")
        props.put("auto.commit.interval.ms", "10000")
        val name = wd.kafkaTopic + jid.toString + "-" + id.toString
        val cons = new FlinkKafkaConsumer010[PRQData](name, new MyDeserializer, props)
          .assignTimestampsAndWatermarks(new MyWaterMarker)
        val ds = FP
          .addSource(cons)
          .setParallelism(1)
        r :+= (id, ds)
      }
      r
    }
    // start here
    val filenames = toc
      .map(s => s.split(" ", 2))
      .map(r => (r.head, r.last)).toMap
    val ids = filenames.keys.map(_.toInt).toArray
    val stuff = readFromKafka(ids)
    stuff.foreach{x =>
      val (id, ds) = x
      // writeCount2(ds, filenames(id.toString))
      val sam = ds
        // .countWindowAll(32*1024)
        .timeWindowAll(Time.seconds(30))
        // .windowAll(GlobalWindows.create)
        // .evictor(new MyEvictor)
        // .trigger(new MyTrigger)
        .apply(new PRQ2SAMRecord[TimeWindow](roba.sref))
      writeToOF(sam, filenames(id.toString))
    }
    FP.execute
    ofs.foreach(_.finalizeGlobal(1))
  }
}

object runWriter {
  def main(args: Array[String]) {
    val propertiesFile = "conf/bclconverter.properties"
    val param = ParameterTool.fromPropertiesFile(propertiesFile)
    val numTasks = param.getInt("numWriters") // concurrent flink tasks to be run
    implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(numTasks))
    // implicit val timeout = Timeout(30 seconds)

    val wd = new WData(param)

    val rg = new scala.util.Random
    val cp = new ConsProps("conconsumer10.")
    cp.put("auto.offset.reset", "earliest")
    cp.put("enable.auto.commit", "true")
    cp.put("auto.commit.interval.ms", "1000")
    val conConsumer = new KafkaConsumer[Int, String](cp)

    conConsumer.subscribe(List(wd.kafkaControl))
    var jobs = List[Future[Any]]()
    while (true) {
      val records = conConsumer.poll(3000)
      records.foreach(r => println("Adding job " + r.key))
      jobs ++= records.map(r => Future{
        val rw = new Writer(wd)
        rw.kafka2cram(r.key, r.value.split("\n"))
      })
    }
    conConsumer.close
  }
}
