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
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows, GlobalWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{Window, TimeWindow, GlobalWindow}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.apache.flink.streaming.util.serialization.{KeyedSerializationSchema, KeyedDeserializationSchema}
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
import org.apache.flink.streaming.api.windowing.evictors.Evictor
import org.apache.flink.streaming.api.windowing.evictors.Evictor.EvictorContext
import org.apache.flink.streaming.api.windowing.triggers.Trigger
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue
import bclconverter.reader.Reader.{Block, PRQData}

class MyEvictor[W <: Window] extends Evictor[(Int, PRQData), W] {
  override
  def evictBefore(els : java.lang.Iterable[TimestampedValue[(Int, PRQData)]], size : Int, win : W, ec : EvictorContext) = {
    var it = els.iterator
    while (it.hasNext) {
      val e = it.next.getValue._2._1.size
      if (e == 1)
        it.remove
    }    
  }
  override
  def evictAfter(els : java.lang.Iterable[TimestampedValue[(Int, PRQData)]], size : Int, win : W, ec : EvictorContext) = {}
}

class MyPRQDeserializer extends KeyedDeserializationSchema[(Int, PRQData)] {
  // Main methods
  override def getProducedType = TypeInformation.of(classOf[(Int, PRQData)])
  override def isEndOfStream(el : (Int, PRQData)) : Boolean = {
    if (el._1 == -1){
      println("EOS reached")
      true
    }
    else
      false
  }
  override def deserialize(key : Array[Byte], data : Array[Byte], topic : String, partition : Int, offset : Long) : (Int, PRQData) = {
    val (s1, r1) = data.splitAt(4)
    val (p1, d1) = r1.splitAt(toInt(s1))
    val (s2, r2) = d1.splitAt(4)
    val (p2, d2) = r2.splitAt(toInt(s2))
    val (s3, r3) = d2.splitAt(4)
    val (p3, d3) = r3.splitAt(toInt(s3))
    val (s4, r4) = d3.splitAt(4)
    val (p4, d4) = r4.splitAt(toInt(s4))
    val (s5, p5) = d4.splitAt(4)
    (toInt(key), (p1, p2, p3, p4, p5))
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
    def finalizeOutput(p : String) = {
      val opath = new HPath(p)
      val job = Job.getInstance(new HConf)
      MapreduceFileOutputFormat.setOutputPath(job, opath)
      val hof = new HadoopOutputFormat(new SAM2CRAM, job)
      hof.finalizeGlobal(1)
    }
    def writeToOF(x : (DataStream[SAMRecordWritable], String)) = {
      val opath = new HPath(x._2)
      val job = Job.getInstance(new HConf)
      MapreduceFileOutputFormat.setOutputPath(job, opath)
      val hof = new HadoopOutputFormat(new SAM2CRAM, job)
      x._1.map(s => (new LongWritable(123), s)).writeUsingOutputFormat(hof).setParallelism(1)
    }
    def readFromKafka : DataStream[(Int, PRQData)] = {
      val props = new ConsProps("outconsumer10.")
      // props.put("auto.offset.reset", "earliest")
      props.put("enable.auto.commit", "true")
      props.put("auto.commit.interval.ms", "1000")
      val cons = new FlinkKafkaConsumer010[(Int, PRQData)](wd.kafkaTopic + jid.toString, new MyPRQDeserializer, props)
      val ds = FP
        .addSource(cons)
      ds
    }
    // start here
    val stuff = readFromKafka
    val sam = stuff
      .keyBy(0)
      .window(GlobalWindows.create)
      .trigger(MyCountTrigger.of(32*1024))
      .evictor(new MyEvictor[GlobalWindow])
      .apply(new PRQ2SAMRecord[GlobalWindow](roba.sref))
    val filenames = toc.map(s => s.split(" ", 2))
    val splitted = sam.split(x => List(x._1.toString))
    val jobs = filenames.map(f => (splitted.select(f.head).map(_._2), f.last))
    jobs.foreach(writeToOF)
    FP.execute
    filenames.map(_.last).foreach(finalizeOutput)
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
    // cp.put("auto.offset.reset", "earliest")
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
