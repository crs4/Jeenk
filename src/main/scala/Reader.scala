package bclconverter.reader

import com.typesafe.config.ConfigFactory
import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.Executors
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010
import org.apache.flink.streaming.connectors.kafka.partitioner.KafkaPartitioner
import org.apache.flink.streaming.util.serialization.{KeyedSerializationSchema, SimpleStringSchema}
import org.apache.hadoop.conf.{Configuration => HConf}
import org.apache.hadoop.fs.{FileSystem, FSDataInputStream, FSDataOutputStream, Path => HPath}
import org.apache.kafka.clients.producer.{Partitioner, KafkaProducer, ProducerRecord}
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Await, Future}
import scala.io.Source
import scala.xml.{XML, Node}

import Reader.{Block, PRQData}

class MyKSerializer extends KeyedSerializationSchema[PRQData] {
  def toBytes(i : Int) : Array[Byte] = {
    ByteBuffer.allocate(4).putInt(i).array
  }
  override def serializeValue(x : PRQData) : Array[Byte] = {
    val r = toBytes(x._1.size) ++ x._1 ++
      toBytes(x._2.size) ++ x._2 ++
      toBytes(x._3.size) ++ x._3 ++
      toBytes(x._4.size) ++ x._4 ++
      toBytes(x._5.size) ++ x._5
    return r
  }
  override def serializeKey(x : PRQData) : Array[Byte] = null
  override def getTargetTopic(x : PRQData) : String = null
}

class MyPartitioner(max : Int) extends KafkaPartitioner[PRQData] {
  var p = -1
  override def partition(next : PRQData, serKey : Array[Byte], serVal : Array[Byte], parts : Int) : Int = {
    p = (p + 1) % max
    println(s"partition $p of $max")
    return p
  }
}

object ProdProps {
  val rg = new scala.util.Random
}

class ProdProps(pref : String) extends Properties {
  private val pkeys = Seq("bootstrap.servers", "acks", "compression.type", "key.serializer", "value.serializer",
    "batch.size", "linger.ms", "request.timeout.ms").map(pref + _)

  lazy val typesafeConfig = ConfigFactory.load()

  pkeys.map{ key =>
    if (typesafeConfig.hasPath(key))
      put(key.replace(pref, ""), typesafeConfig.getString(key))
  }
  put("client.id", "robo" + ProdProps.rg.nextLong.toString)
  put("retries", "5")
  put("max.in.flight.requests.per.connection", "1")


  def getCustomString(key: String) = typesafeConfig.getString(key)
  def getCustomInt(key: String) = typesafeConfig.getInt(key)
}


class fuzzyIndex(sm : Map[(Int, String), String], mm : Int, undet : String) extends Serializable {
  def hamDist(a : String, b : String) : Int = {
    val va = a.getBytes
    val vb = b.getBytes
    var cow = 0
    va.indices.foreach(i => if (va(i) != vb(i)) cow += 1)
    cow
  }
  def findMatch(k : (Int, String)) : String = {
    val (lane, pat) = k
    val m = inds.filter(_._1 == lane)
      .map(x => (x, hamDist(pat, x._2)))
      .filter(_._2 <= mm).toArray.sortBy(_._2)
    val r = {
      // no close match
      if (m.isEmpty)
	undet
      else // return closest match
	m.head._1._2
      }
    seen += (k -> r)
    return r
  }
  def getIndex(k : (Int, String)) : String = {
    seen.getOrElse(k, findMatch(k))
  }
  // main
  val inds = sm.keys
  var seen = Map[(Int, String), String]()
  inds.foreach(k => seen += (k -> k._2))
}

class RData extends Serializable{
  var header : Block = Array()
  var ranges : Seq[Seq[Int]] = null
  var index : Seq[Seq[Int]] = null
  var fuz : fuzzyIndex = null
  // parameters
  var root : String = null
  var fout : String = null
  var bdir = "Data/Intensities/BaseCalls/"
  var adapter : String = null
  var bsize = 2048
  var mismatches = 1
  var undet = "Undetermined"
  var rgrouping = 1
  var flinkpar = 1
  var kafkaTopic : String = "prq"
  var kafkaControl : String = "kcon"
  def setParams(param : ParameterTool) = {
    root = param.getRequired("root")
    fout = param.getRequired("fout")
    bdir = param.get("bdir", bdir)
    adapter = param.get("adapter", adapter)
    bsize = param.getInt("bsize", bsize)
    mismatches = param.getInt("mismatches", mismatches)
    undet = param.get("undet", undet)
    rgrouping = param.getInt("rgrouping", rgrouping)
    flinkpar = param.getInt("readerflinkpar", flinkpar)
    kafkaTopic = param.get("kafkaTopic", kafkaTopic)
    kafkaControl = param.get("kafkaControl", kafkaControl)
  }
}

object Reader {
  type Block = Array[Byte]
  type PRQData = (Block, Block, Block, Block, Block)
  val conProducer = new KafkaProducer[Int, String](new ProdProps("conproducer10."))
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

class Reader() extends Serializable {
  val rd = new RData
  var sampleMap = Map[(Int, String), String]()
  var filenames = Map[(Int, String), String]()
  var f2id = Map[String, Int]()
  var lanes = 0
  // processes tile and produces PRQ
  def BCLprocess(input : Seq[(Int, Int)]) = {
    val FP = StreamExecutionEnvironment.getExecutionEnvironment
    FP.setParallelism(rd.flinkpar)
    def kafkize(x : (DataStream[PRQData], Int)) = {
      val ds = (x._1)
      val key = x._2
      val name = rd.kafkaTopic + "-" + key.toString
      FlinkKafkaProducer010.writeToKafkaWithTimestamps(
        ds.javaStream,
        name,
        new MyKSerializer,
        new ProdProps("outproducer10."),
        new MyPartitioner(runReader.kafkapar)
      )
    }
    def procReads(input : (Int, Int)) : Seq[(DataStream[PRQData], Int)] = {
      val (lane, tile) = input
      println(s"Processing lane $lane tile $tile")

      val in = FP.fromElements(input)
      val bcl = in.flatMap(new PRQreadBCL(rd)).rebalance
      val stuff = bcl
        .split {
	input : (Block, Block, Block, Block) =>
	new String(input._1) match {
          case x => List(rd.fuz.getIndex((lane, x)))
	}
      }
      val output = filenames
        .filterKeys(_._1 == lane)
        .keys.map{ k =>
	val ds = stuff.select(k._2).map(x => (x._2, x._3, x._4))
	  .map(new toPRQ)

	val ho = filenames(k)
	(ds, f2id(ho))
      }.toSeq
      return output
    }
    // START
    val stuff = input
      .flatMap(procReads)
    stuff.foreach(kafkize)
    FP.execute
  }
  // send EOS to each kafka partition, for each topic
  def sendEOS = {
    val FP = StreamExecutionEnvironment.getExecutionEnvironment
    FP.setParallelism(1)
    val k : Block = Array(13)
    val eos : PRQData = (k, k, k, k, k)
    val EOS : DataStream[PRQData] = FP.fromCollection(Array.fill(runReader.kafkapar)(eos))
    f2id.values.foreach{id =>
      FlinkKafkaProducer010.writeToKafkaWithTimestamps(
        EOS.javaStream,
        rd.kafkaTopic + "-" + id.toString,
        new MyKSerializer,
        new ProdProps("outproducer10."),
        new MyPartitioner(runReader.kafkapar)
      )
    }
    FP.execute
  }
  def setFilenames = {
    // Uncomment next lines if you want "Undetermined" reads in the output as well
    // for (lane <- 1 to lanes){
    //   filenames ++= Map((lane, rd.undet) -> new String(f"${rd.fout}${rd.undet}"))
    // }
    filenames ++= sampleMap
      .map {
      case (k, pref) => ((k._1, k._2) -> new String(f"${rd.fout}${pref}"))
    }
  }
  def sendTOC = {
    val fns = filenames.values.toArray
    f2id = fns.indices.map(i => (fns(i), i)).toMap
    val toclines = f2id.toArray.map{case (n, i) => s"$i $n"}
    Reader.conProducer.send(new ProducerRecord(rd.kafkaControl, toclines.mkString("\n")))
  }
  def readSampleNames = {
    // open SampleSheet.csv
    val path = new HPath(rd.root + "SampleSheet.csv")
    val fs = Reader.MyFS(path)
    val in = fs.open(path)
    val fsize = fs.getFileStatus(path).getLen
    val coso = Source.createBufferedSource(in).getLines.map(_.trim).toArray
    val tags = coso.indices.map(i => (i, coso(i))).filter(x => x._2.startsWith("[")) :+ (coso.size, "[END]")
    val drange = tags.indices.dropRight(1)
      .map(i => ((tags(i)._1, tags(i + 1)._1), tags(i)._2))
      .find(_._2 == "[Data]")
    val dr = drange match {
      case Some(x) => x._1
      case None => throw new Error("No [Data] section in SampleSheet.csv")
    }
    val csv = coso.slice(dr._1, dr._2).drop(2)
      .map(_.split(","))
    csv.foreach(l => sampleMap += (l(0).toInt, l(6)) -> l(1))
    rd.fuz = new fuzzyIndex(sampleMap, rd.mismatches, rd.undet)
    in.close
    setFilenames
  }
  def getAllJobs : Seq[(Int, Int)] = {
    // open runinfo.xml
    val xpath = new HPath(rd.root + "RunInfo.xml")
    val fs = Reader.MyFS(xpath)
    val xin = fs.open(xpath)
    val xsize = fs.getFileStatus(xpath).getLen
    val xbuf = new Array[Byte](xsize.toInt)
    val xml = XML.load(xin)
    // read parameters
    val instrument = (xml \ "Run" \ "Instrument").text
    val runnum = (xml \ "Run" \ "@Number").text
    val flowcell = (xml \ "Run" \ "Flowcell").text
    rd.header = s"@$instrument:$runnum:$flowcell:".getBytes
    // reads and indexes
    val reads = (xml \ "Run" \ "Reads" \ "Read")
      .map(x => ((x \ "@NumCycles").text.toInt, (x \ "@IsIndexedRead").text))
    val fr = reads.map(_._1).scanLeft(1)(_ + _)
    rd.ranges = reads.indices.map(i => (fr(i), fr(i + 1), reads(i)._2))
      .filter(_._3 == "N").map(x => Range(x._1, x._2))
    rd.index = reads.indices.map(i => (fr(i), fr(i + 1), reads(i)._2))
      .filter(_._3 == "Y").map(x => Range(x._1, x._2))

    val layout = xml \ "Run" \ "FlowcellLayout"
    lanes = (layout \ "@LaneCount").text.toInt
    val surfaces = (layout \ "@SurfaceCount").text.toInt
    val swaths = (layout \ "@SwathCount").text.toInt
    val tiles = (layout \ "@TileCount").text.toInt

    val r = for (l <- 1 to lanes; sur <- 1 to surfaces; sw <- 1 to swaths; t <- 1 to tiles) yield (l, sur * 1000 + sw * 100 + t)
    xin.close
    r // Seq[(lane, tile)]
  }
}

object runReader {
  var kafkapar = 1
  def main(args: Array[String]) {
    val propertiesFile = "conf/bclconverter.properties"
    val param = ParameterTool.fromPropertiesFile(propertiesFile)
    val numTasks = param.getInt("numReaders") // concurrent flink tasks to be run
    kafkapar = param.getInt("kafkapar", kafkapar)
    implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(numTasks))
    // implicit val timeout = Timeout(30 seconds)

    val reader = new Reader
    reader.rd.setParams(param)
    reader.readSampleNames
    reader.sendTOC

    val w = reader.getAllJobs
    val rgrouping = reader.rd.rgrouping
    val tasks = w.grouped(rgrouping).map(x => Future{reader.BCLprocess(x)})
    val aggregated = Future.sequence(tasks)
    Await.result(aggregated, Duration.Inf)
    reader.sendEOS
    Reader.conProducer.close
  }
}
