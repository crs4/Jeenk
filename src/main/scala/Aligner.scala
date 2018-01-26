package bclconverter.aligner

import com.typesafe.config.ConfigFactory
import htsjdk.samtools.SAMRecord
import java.util.concurrent.{Executors, TimeoutException}
import org.apache.flink.api.java.utils.ParameterTool
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
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Await, Future}

import bclconverter.reader.Reader.{Block, PRQData}
import bclconverter.kafka.{MySSerializer, MyPartitioner, ProdProps, ConsProps, MyDeserializer, MySDeserializer}

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

class PList(val param : ParameterTool) extends Serializable{
  // parameters
  val root = param.getRequired("root")
  val rapiwin = param.getInt("rapiwin", 1024)
  val flinkpar = param.getInt("alignerflinkpar", 1)
  val kafkapar = param.getInt("kafkapar", 1)
  val rapipar = param.getInt("rapipar", 1)
  val agrouping = param.getInt("agrouping", 1)
  val kafkaServer = param.get("kafkaServer", "127.0.0.1:9092")
  val kafkaTopic = param.get("kafkaTopic", "flink-prq")
  val kafkaAligned = param.get("kafkaAligned", "flink-aligned")
  val kafkaControl = param.get("kafkaControl", "flink-con")
  val stateBE = param.getRequired("stateBE")
  val sref = param.getRequired("reference")
  val alignerTimeout = param.getInt("alignerTimeout", 0)
}

class miniAligner(pl : PList, ind : (Int, Int)) {
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
  def sendAligned(x : (DataStream[SAMRecord], Int)) = {
    val name = pl.kafkaAligned + "-" + x._2.toString
    val part : java.util.Optional[FlinkKafkaPartitioner[SAMRecord]] = java.util.Optional.of(new MyPartitioner[SAMRecord](pl.kafkapar))
    val kprod = new FlinkKafkaProducer011(
      name,
      new MySSerializer,
      new ProdProps("outproducer11.", pl.kafkaServer),
      part
    )
    x._1.addSink(kprod)
      .name(name)
  }
  def add(id : Int, topicname : String, filename : String) {
    jobs ::= (id, topicname, filename)
  }
  def doJob(id : Int, topicname : String, filename : String) = {
    val props = new ConsProps("outconsumer11.", pl.kafkaServer)
    props.put("auto.offset.reset", "earliest")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "10000")
    val cons = new FlinkKafkaConsumer011[(Int, PRQData)](topicname, new MyDeserializer(pl.flinkpar), props)
    val ds = env
      .addSource(cons)
      .setParallelism(pl.kafkapar)
      .assignTimestampsAndWatermarks(new MyWaterMarker[(Int, PRQData)])
      .name(topicname)
    val sam = ds
      .keyBy(_._1)
      .timeWindow(Time.milliseconds(pl.rapiwin * pl.flinkpar / pl.kafkapar))
      .apply(new bclconverter.sam.PRQAligner[TimeWindow, Int](pl.sref, pl.rapipar))

    // send data to topic
    sendAligned(sam, id)
  }
  // send EOS to each kafka partition, for each topic
  def sendEOS(id : Int) = {
    env.setParallelism(1)
    val name = pl.kafkaAligned + "-" + id.toString
    val eos  = new SAMRecord(null)
    eos.setReadName(MySDeserializer.eos_tag)
    val EOS : DataStream[SAMRecord] = env.fromCollection(Array.fill(pl.kafkapar)(eos))
    EOS.name("EOS")
    val part : java.util.Optional[FlinkKafkaPartitioner[SAMRecord]] = java.util.Optional.of(new MyPartitioner[SAMRecord](pl.kafkapar))
    val kprod = new FlinkKafkaProducer011(
      name,
      new MySSerializer,
      new ProdProps("outproducer11.", pl.kafkaServer),
      part
    )
    EOS.addSink(kprod)
       .name(name)
  }
  def go = {
    jobs.foreach(j => doJob(j._1, j._2, j._3))
    env.execute(s"Aligner ${ind._1}/${ind._2}")
    jobs.foreach(j => sendEOS(j._1))
    env.execute(s"Send EOS ${ind._1}/${ind._2}")
  }
}

class Aligner(pl : PList) {
  val conProducer = new KafkaProducer[Int, String](new ProdProps("conproducer11.", pl.kafkaServer))
  def kafkaAligner(toc : Array[String]) : Iterable[miniAligner] = {
    val filenames = toc
      .map(s => s.split(" ", 2))
      .map(r => (r.head, r.last)).toMap
    def RW(ids : Iterable[Int], ind : (Int, Int)) : miniAligner = {
      val mw = new miniAligner(pl, ind)
      ids.foreach{
        id =>
        val topicname = pl.kafkaTopic + "-" + id.toString
        mw.add(id, topicname, filenames(id.toString))
      }
      mw
    }
    conProducer.send(new ProducerRecord(pl.kafkaControl, 1, toc.mkString("\n")))
    val ids = filenames.keys.map(_.toInt)
    val g = ids.grouped(pl.agrouping).toArray
    val n = g.size
    g.indices.map(i => RW(g(i), (i+1, n)))
  }
}

object runAligner {
  def main(args: Array[String]) {
    val pargs = ParameterTool.fromArgs(args)
    val propertiesFile = pargs.getRequired("properties")
    val pfile = ParameterTool.fromPropertiesFile(propertiesFile)
    val params = pfile.mergeWith(pargs)

    val numTasks = params.getInt("numAligners") // concurrent flink tasks to be run
    implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(numTasks))
    // implicit val timeout = Timeout(30 seconds)

    val pl = new PList(params)
    val rg = new scala.util.Random
    val cp = new ConsProps("conconsumer11.", pl.kafkaServer)
    cp.put("auto.offset.reset", "earliest")
    cp.put("enable.auto.commit", "true")
    cp.put("auto.commit.interval.ms", "10000")
    val conConsumer = new KafkaConsumer[Int, String](cp)
    val rw = new Aligner(pl)
    conConsumer.subscribe(List(pl.kafkaControl))
    var jobs = List[Future[Any]]()
    val startTime = java.time.Instant.now.getEpochSecond
    var goOn = true

    while (goOn) {
      val records = conConsumer.poll(3000)
      jobs ++= records
        .filter(_.key == 0)
        .flatMap
      {
	r =>
	  rw.kafkaAligner(r.value.split("\n"))
	    .map(j => Future{j.go})
      }
      // stay in loop until jobs are done or timeout not expired
      val now = java.time.Instant.now.getEpochSecond
      goOn = pl.alignerTimeout <= 0 || now < startTime + pl.alignerTimeout
      val aggregated = Future.sequence(jobs)
      try {
        Await.ready(aggregated, 100 millisecond)
      } catch {
        case e: TimeoutException => goOn = true
      }
    }
    conConsumer.close
  }
}

