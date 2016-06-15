package bclconverter.bclreader

import bclconverter.{FlinkStreamProvider => FP, Fenv}
import java.io.OutputStream
import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.hadoop.conf.{Configuration => HConf}
import org.apache.hadoop.fs.{FileSystem, FSDataInputStream, FSDataOutputStream, Path => HPath, LocatedFileStatus}
import scala.io.Source
import scala.xml.{XML, Node}
import scala.collection.parallel._
import org.apache.hadoop.io.compress.zlib.ZlibFactory
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.io.compress.zlib.ZlibCompressor

import Reader.Block

class Fout(filename : String) extends OutputFormat[Block] {
  var writer : OutputStream = null
  def close() = {
    writer.close
  }
  def configure(conf : Configuration) = {
  }
  def open(taskNumber : Int, numTasks : Int) = {
    val compressor = new ZlibCompressor(
      ZlibCompressor.CompressionLevel.BEST_SPEED,
      ZlibCompressor.CompressionStrategy.DEFAULT_STRATEGY,
      ZlibCompressor.CompressionHeader.GZIP_FORMAT,
      64 * 1024)

    val ccf = new CompressionCodecFactory(new HConf)
    val codec = ccf.getCodecByName("gzip")
    val path = new HPath(filename + codec.getDefaultExtension)
    val fs = FileSystem.get(new HConf)
    if (fs.exists(path)) 
      fs.delete(path, true)
    val out = fs.create(path)
    
    writer = codec.createOutputStream(out, compressor)
  }
  def writeRecord(rec : Block) = {
    writer.write(rec)
  }
}

class RData extends Serializable{
  var header : Block = Array()
  var ranges : Seq[Seq[Int]] = null
  var index : Seq[Seq[Int]] = null
  var fuz : fuzzyIndex = null
}

object Reader {
  type Block = Array[Byte]
  val root = "/u/cesco/dump/data/illumina/"
  val bdir = "Data/Intensities/BaseCalls/"
  val fout = "/u/cesco/dump/data/out/mio/"
  val adapter = "CTTCCTCTACA"
  val bsize = 2048
  val mismatches = 1
  val undet = "Undetermined"
}

class Reader extends Serializable{
  val rd = new RData
  var sampleMap = Map[(Int, String), String]()
  // process tile
  def process(input : (Int, Int)) = {
    println(s"------> Processing lane ${input._1} tile ${input._2}")
    val mFP = new Fenv
    def procReads(input : (Int, Int)) : Seq[(DataStream[Block], OutputFormat[Block])] = {
      val (lane, tile) = input
      val in = mFP.env.fromElements(input)
      val bcl = in.flatMap(new readBCL(rd)).split{
        input : (Block, Int, Block, Block) =>
        (input._2) match {
          case 0 => List("R1")
          case 1 => List("R2")
        }
      }
      val rreads = Array("R1", "R2")
      var houts = rreads.map{ rr =>
        sampleMap.filterKeys(_._1 == lane)
          .map {
	  case (k, pref) => ((k._1, k._2) -> new Fout(f"${Reader.fout}${pref}_L${k._1}%03d_${tile}-${rr}.fastq"))
        }
      }
      rreads.indices.foreach{
        i => (houts(i) += (lane, Reader.undet) -> new Fout(f"${Reader.fout}Undetermined_L${lane}%03d_${tile}-${rreads(i)}.fastq"))
      }
      val stuff = rreads.indices.map{ i =>
        bcl.select(rreads(i))
          .map(x => (x._1, x._3, x._4))
          .split{
	  input : (Block, Block, Block) =>
	  new String(input._1) match {
            case x => List(rd.fuz.getIndex((lane, x)))
	  }
        }
      }
      val output = rreads.indices.flatMap{ i =>
        houts(i).keys.map{ k =>
	  val ds = stuff(i).select(k._2).map(x => (x._2, x._3))
	    .map(new toFQ)
	  // .map(new delAdapter(Reader.adapter.getBytes))
	    .map(new Flatter)
	  val ho = houts(i)(k)
	  (ds, ho)
        }
      }
      return output
    }
    val stuff = procReads(input)
    stuff.foreach(x => x._1.writeUsingOutputFormat(x._2).setParallelism(1))
    mFP.env.execute
  }
  def readLane(lane : Int, outdir : String) : Seq[(Int, Int)] = {
    val fs = FileSystem.get(new HConf)
    val ldir = f"${Reader.root}${Reader.bdir}L${lane}%03d/"
    val starttiles = 1101
    val endtiles = 2000

    val tiles = Range(starttiles, endtiles)
      .map(x => s"s_${lane}_$x.bcl.gz")
      .map(x => (x, new HPath(s"$ldir/C1.1/$x")))
      .filter(x => fs.isFile(x._2)).map(_._1)
      .toArray

    val tnum = tiles.map(t => t.substring(4,8).toInt)
    tnum.map((lane, _))
  }
  def readSampleNames = {
    // open runinfo.xml
    val path = new HPath(Reader.root + "SampleSheet.csv")
    val fs = FileSystem.get(new HConf)
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
    rd.fuz = new fuzzyIndex(sampleMap)
  }
  def getAllJobs : Seq[(Int, Int)] = {
    // open runinfo.xml
    val xpath = new HPath(Reader.root + "RunInfo.xml")
    val fs = FileSystem.get(new HConf)
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
    // val lanes = (xml \ "Run" \ "AlignToPhiX" \\ "Lane").map(_.text.toInt)
    val lanes = Range(1, 9)
    // get data from each lane
    lanes.flatMap(l => readLane(l, Reader.fout))
  }
}

object test {
  def main(args: Array[String]) {
    val reader = new Reader
    reader.readSampleNames
    val w = reader.getAllJobs.toArray.par

    val procs = 2 // number of flink tasks to issue at a time
    w.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(procs))
    w.foreach(reader.process)
  }
}
