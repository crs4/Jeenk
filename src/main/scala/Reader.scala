/*
 * Copyright (c) 2018 CRS4
 *
 * This file is part of Jeenk.
 *
 * Jeenk is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Jeenk is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public
 * License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Jeenk.  If not, see <http://www.gnu.org/licenses/>.
 */

package it.crs4.jeenk.reader

import com.typesafe.config.ConfigFactory
import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.Executors
import org.apache.flink.api.java.utils.ParameterTool
// import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.extensions._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner
import org.apache.flink.streaming.util.serialization.{KeyedSerializationSchema, SimpleStringSchema}
import org.apache.hadoop.conf.{Configuration => HConf}
import org.apache.hadoop.fs.{FileSystem, Path => HPath}
import org.apache.kafka.clients.admin._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Await, Future}
import scala.collection.JavaConversions._
import scala.io.Source
import scala.xml.{XML, Node}

import it.crs4.jeenk.kafka.{MyKSerializer, MyPartitioner, ProdProps}
import it.crs4.jeenk.conf.Params
import it.crs4.jeenk.conf.Params.{Block, PRQData}

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


class miniReader(var rd : Params, var filenames : Map[(Int, String), String], var f2id : Map[String, Int]) extends Serializable {
  // vars
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.getConfig.setGlobalJobParameters(rd.param)
  env.setParallelism(rd.rflinkpar)
  // serialization
  private def writeObject(out : java.io.ObjectOutputStream) {
    out.writeObject(rd)
    out.writeObject(filenames)
    out.writeObject(f2id)
  }
  private def readObject(in : java.io.ObjectInputStream){
    rd = in.readObject.asInstanceOf[Params]
    filenames = in.readObject.asInstanceOf[Map[(Int, String), String]]
    f2id = in.readObject.asInstanceOf[Map[String, Int]]
  }
  def createKTopic(topic : String) = {
    val props = new Properties
    props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, rd.kafkaServer)
    val adminClient = AdminClient.create(props)
    val newTopic = new NewTopic(topic, rd.rkafkaout, 1)
    try {
      val fut = adminClient.createTopics(List(newTopic))
      fut.all.get
    } catch {
      case _ : java.util.concurrent.ExecutionException => // topic already exists
    }
  }
  def kafkize(x : (DataStream[PRQData], Int)) = {
    val ds = x._1
    val key = x._2
    val name = rd.kafkaTopic + "-" + key.toString
    createKTopic(name)
    val part : java.util.Optional[FlinkKafkaPartitioner[PRQData]] = java.util.Optional.of(new MyPartitioner[PRQData](rd.rkafkaout))
    val kprod = new FlinkKafkaProducer011(
      name,
      new MyKSerializer,
      new ProdProps("outproducer11.", rd.kafkaServer),
      part
    )
    ds.addSink(kprod)
      .name(name)
  }
  def procReads(input : (Int, Int)) : Seq[(DataStream[PRQData], Int)] = {
    val (lane, tile) = input
    val in = env.fromElements(input)
    val bcl = in.flatMap(new PRQreadBCL(rd))
    .name(s"Lane $lane tile $tile")
    .rebalance
    val stuff = bcl
    .split {
      input : (Block, Block, Block, Block) =>
	new String(input._1) match {
          case x => List(rd.fuz.getIndex((lane, x)))
	}
    }
    val output = filenames
    .filterKeys(_._1 == lane)
    .keys.map{k =>
      val tag = k._2
      val ds = stuff.select(tag).map(x => (x._2, x._3, x._4))
      .map(new toPRQ)
      // .name(s"${sampleMap((lane, tag))}")
      val ho = filenames(k)
      (ds, f2id(ho))
    }.toSeq
    return output
  }
  // START
  def run(input : Seq[(Int, Int)], ind : (Int, Int)) = { 
    val stuff = input
      .flatMap(procReads)
    stuff.foreach(kafkize)
    env.execute(s"Process BCL ${ind._1}/${ind._2}")
  }
}

class Reader(val rd : Params) {
  val conProducer = new KafkaProducer[Int, String](new ProdProps("conproducer11.", rd.kafkaServer))
  var sampleMap = Map[(Int, String), String]()
  var f2id = Map[String, Int]()
  var lanes = 0
  var filenames = Map[(Int, String), String]()
  def setFilenames = {
    // (Un)comment next lines if you (do not) want "Undetermined" reads in the output as well
    for (lane <- 1 to lanes){
      filenames ++= Map((lane, rd.undet) -> new String(f"${rd.undet}"))
    }
    filenames ++= sampleMap
      .map {
      case (k, pref) => ((k._1, k._2) -> new String(f"${pref}"))
    }
  }
  def sendTOC = {
    val fns = filenames.values.toArray
    f2id = fns.indices.map(i => (fns(i), i)).toMap
    val toclines = f2id.toArray.map{case (n, i) => s"$i $n"}
    conProducer.send(new ProducerRecord(rd.kafkaControlPRQ, 0, toclines.mkString("\n")))
  }
  // send EOS to each kafka partition, for each topic
  def sendEOS = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val k : Block = Array(13)
    val eos : PRQData = (k, k, k, k, k)
      val EOS : DataStream[PRQData] = env.fromCollection(Array.fill(rd.rkafkaout)(eos))
    EOS.name("EOS")
    f2id.values.foreach{id =>
      val part : java.util.Optional[FlinkKafkaPartitioner[PRQData]] = java.util.Optional.of(new MyPartitioner[PRQData](rd.rkafkaout))
      val kprod = new FlinkKafkaProducer011(
	rd.kafkaTopic + "-" + id.toString,
	new MyKSerializer,
	new ProdProps("outproducer11.", rd.kafkaServer),
	part
      )
      EOS.addSink(kprod)
		      }
    env.execute("Send EOS's")
  }
  def readSampleNames = {
    // open SampleSheet.csv
    val path = new HPath(rd.samplePath)
    val fs = Params.MyFS(path)
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
    val fs = Params.MyFS(xpath)
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
  def getMinireader : miniReader = {
    new miniReader(rd, filenames, f2id)
  }
}

object runReader {
  def main(args: Array[String]) {
    val pargs = ParameterTool.fromArgs(args)
    val propertiesFile = pargs.getRequired("properties")
    val pfile = ParameterTool.fromPropertiesFile(propertiesFile)
    val params = new Params(pfile.mergeWith(pargs))

    implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(params.numReaders))
    // implicit val timeout = Timeout(30 seconds)

    val reader = new Reader(params)
    val w = reader.getAllJobs
    reader.readSampleNames
    reader.sendTOC

    val rgrouping = reader.rd.rgrouping
    val tasks = w.grouped(rgrouping).toArray
    val n = tasks.size
    val jobs = tasks.indices.map(i => Future{reader.getMinireader.run(tasks(i), (i+1, n))})
    val aggregated = Future.sequence(jobs)
    Await.result(aggregated, Duration.Inf)
    reader.sendEOS
    reader.conProducer.close
  }
}
