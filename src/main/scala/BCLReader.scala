package bclconverter.bclreader

import bclconverter.{FlinkStreamProvider => FP}
import java.nio.{ByteBuffer, ByteOrder}
import java.io.OutputStream
import org.apache.flink.api.common.functions.{MapFunction, FlatMapFunction, ReduceFunction, GroupReduceFunction}
import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.hadoop.mapreduce.{HadoopInputFormat, HadoopOutputFormat}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.apache.hadoop.conf.{Configuration => HConf}
import org.apache.hadoop.fs.{FileSystem, FSDataInputStream, FSDataOutputStream, Path => HPath, LocatedFileStatus}
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.compress.{GzipCodec, SnappyCodec, Lz4Codec, CompressionCodec, CompressionCodecFactory}
import org.apache.hadoop.io.compress.zlib.ZlibCompressor
import org.apache.hadoop.mapreduce.RecordWriter
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat => HFileInputFormat, FileSplit}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat => HFileOutputFormat, NullOutputFormat}
import org.apache.hadoop.mapreduce.{InputSplit, TaskAttemptContext, RecordReader, Job, JobContext}
import scala.xml.{XML, Node}

import BCL.Block

object BCL {
  type Block = Array[Byte]
}

class Fout(filename : String) extends OutputFormat[Block] {
  var writer : OutputStream = null
  def close() = {
    writer.close
  }
  def configure(conf : Configuration) = {
  }
  def open(taskNumber : Int, numTasks : Int) = {
    // val codec = new SnappyCodec //GzipCodec
    val path = new HPath(filename) // + codec.getDefaultExtension)
    val fs = FileSystem.get(new HConf)
    if (fs.exists(path)) 
      fs.delete(path, true)
    val out = fs.create(path)
    /*
    val compressor = new ZlibCompressor(ZlibCompressor.CompressionLevel.BEST_SPEED,
      ZlibCompressor.CompressionStrategy.DEFAULT_STRATEGY,
      ZlibCompressor.CompressionHeader.GZIP_FORMAT,
      64*1024)
      writer = codec.createOutputStream(out) // (out, compressor)
    */
    writer = out
  }
  def writeRecord(rec : Block) = {
    writer.write(rec)
  }
}

object toFQ {
  var header : Block = Array()
  val mid = "\n+\n".getBytes
  val newl = "\n".getBytes
  val dict = CreateTable.getArray
  val toBase : Block = Array('A', 'C', 'G', 'T')
}

class toFQ extends MapFunction[(Block, Block), Block] {
  var blocksize : Int = _
  var bbin : ByteBuffer = _
  def compact(l : Long) : Int = {
    val i0 = (l & 0x000000000000000Fl)
    val i1 = (l & 0x0000000000000F00l) >>>  6
    val i2 = (l & 0x00000000000F0000l) >>> 12
    val i3 = (l & 0x000000000F000000l) >>> 18
    val i4 = (l & 0x0000000F00000000l) >>> 24
    val i5 = (l & 0x00000F0000000000l) >>> 30
    val i6 = (l & 0x000F000000000000l) >>> 36
    val i7 = (l & 0x0F00000000000000l) >>> 42
    (i0 + i1 + i2 + i3 + i4 + i5 + i6 + i7).toInt 
  }
  def toB : Block = {
    bbin.rewind
    val bbout = ByteBuffer.allocate(blocksize)
    while(bbin.remaining > 7) {
      val r = bbin.getLong & 0x0303030303030303l
      val o = toFQ.dict(compact(r))
      bbout.putLong(o)
    }
    while(bbin.remaining > 0) {
      val b = bbin.get
      val idx = b & 0x03
      bbout.put(toFQ.toBase(idx))
    }
    bbout.array
  }
  def toQ : Block = {
    bbin.rewind    
    val bbout = ByteBuffer.allocate(blocksize)
    while(bbin.remaining > 7) {
      val r = bbin.getLong
      bbout.putLong(0x2121212121212121l + ((r & 0xFCFCFCFCFCFCFCFCl) >>> 2))
    }
    while(bbin.remaining > 0) {
      val b = bbin.get
      val q = (b & 0xFF) >>> 2
      bbout.put((0x21 + q).toByte)
    }
    bbout.array
  }
  def map(x : (Block, Block)) : Block = {
    val b = x._1
    val h = x._2

    blocksize = b.size
    bbin = ByteBuffer.wrap(b)
    val nB = toB
    val nQ = toQ
    // apply redundant fastq annotation
    b.indices.foreach {i =>
      if (nQ(i) == 0x21.toByte) {
        nQ(i) = 0x23.toByte // #, 0 quality
        nB(i) = 0x4E.toByte // N, no-call
      }
    }
    h ++ nB ++ toFQ.mid ++ nQ ++ toFQ.newl
  }
}

object readBCL {
  /// block size when reading
  val bsize = 2048
  // process tile
  def process(input : (Array[HPath], Int, Int, Int)) : DataStream[Block] = {
    val (hp, lane, tile, rr) = input

    val in = FP.env.fromElements(hp)//.rebalance
    val bcl = in.flatMap(new readBCL(lane, tile, rr))

    bcl
      .map(new toFQ) //(head, cpath, filter))
  }
}

class readBCL(lane : Int, tile : Int, read : Int) extends FlatMapFunction[Array[HPath], (Block, Block)] {
  var st_end = -1l
  val h3 = s" $read:N:".getBytes
  def readBlock(instream : FSDataInputStream) : Block = {
    val buf = new Block(readBCL.bsize)
    val r = instream.read(buf)
    if (r > 0)
      return buf.take(r)
    else
      return null
  }
  def aggBlock(in : Array[FSDataInputStream]) : Array[Block] = {
    if (in.head.getPos >= st_end)
      return null

    in.map(readBlock).transpose
  }
  def flatMap(flist : Array[HPath], out : Collector[(Block, Block)]) = {
    // open bcl files
    val h1 = toFQ.header ++ s"${lane}:${tile}:".getBytes
    val fs = FileSystem.get(new HConf)
    st_end = fs.getFileStatus(flist(0)).getLen
    val streams = flist.map(fs.open)
    streams.foreach(_.seek(4l))
    var buf : Array[Block] = null
    // open filter file
    val filfile = fs.open(new HPath(s"${Reader.root}${Reader.bdir}/L00${lane}/s_${lane}_${tile}.filter"))
    filfile.seek(12)
    // open control file
    val confile = fs.open(new HPath(s"${Reader.root}${Reader.bdir}/L00${lane}/s_${lane}_${tile}.control"))
    confile.seek(12)
    // open locations converter
    val clocs = new Locs(new HPath(s"${Reader.root}${Reader.bdir}/../L00${lane}/s_${lane}_${tile}.clocs"))

    while ({buf = aggBlock(streams); buf != null}) {
      buf.foreach{x => 
	val h2 = clocs.getCoord
	val h4 = s"${confile.read() + confile.read() * 256}:0\n".getBytes
	if (filfile.read() == 1)
	  out.collect(x, h1 ++ h2 ++ h3 ++ h4)}
    }
  }
}

class Locs(path : HPath) {
  val fs = FileSystem.get(new HConf)
  val locsfile = fs.open(path)
  locsfile.seek(1)
  val buf = new Array[Byte](4)
  locsfile.read(buf)
  val numbins = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).getInt
  var bs = -1
  def readBin(n : Int) : Array[Block] = {
    val dx = (1000.5 + (n % 82) * 250).toInt
    val dy = (1000.5 + (n / 82) * 250).toInt
    bs = locsfile.read()
    val r = for (i <- Range(0, bs)) yield {
      val x = locsfile.read()
      val y = locsfile.read()
      (x, y)
    }
    r.toArray.map(a => s"${a._1 + dx}:${a._2 + dy}".getBytes)
  }
  var n = 0
  var i = 0
  var curblock : Array[Block] = null
  def getCoord : Block = {
    while (i >= bs){
      curblock = readBin(n)
      n += 1
      i = 0
    }
    val r = curblock(i)
    i += 1
    return r
  }
}

object Reader {
  val root = "/home/cesco/dump/data/illumina/"
  val fout = "/home/cesco/dump/data/out/mio/"
  val bdir = "Data/Intensities/BaseCalls/"
  var ranges : Seq[Seq[Int]] = null
  def readLane(lane : Int, outdir : String) : Array[((Array[HPath], Int, Int, Int), OutputFormat[Block])] = {
    val fs = FileSystem.get(new HConf)
    val ldir = s"${root}${bdir}L00${lane}/"
    val starttiles = 1101
    val endtiles = 2000

    val tiles = Range(starttiles, endtiles)
      .map(x => s"s_${lane}_$x.bcl")
      .map(x => (x, new HPath(s"$ldir/C1.1/$x")))
      .filter(x => fs.isFile(x._2)).map(_._1)
      .toArray

    val tnum = tiles.map(t => t.substring(4,8).toInt)
    tiles.indices.flatMap { i =>  // for each tile
      val tile = tnum(i)
      ranges.map { cycles =>  // for each read
        val cydirs = cycles
          .map(x => s"$ldir/C$x.1/")
          .map(new HPath(_))
          .filter(fs.isDirectory(_))
          .toArray
        val rr = if (cycles(0) > 1) 2 else 1

        val hp = tiles.map(t => cydirs.map(d => s"$d/$t"))
          .map(t => t.map(s => new HPath(s)))

        val fout = s"${outdir}L00${lane}/s_${lane}_${tile}-R${rr}.fastq"
        val hout = new Fout(fout)

        ((hp(i), lane, tile, rr), hout)
      }
    }.toArray
  }
  def getAllJobs : Seq[((Array[HPath], Int, Int, Int), OutputFormat[Block])] = {
    // open runinfo.xml
    val xpath = new HPath(root + "RunInfo.xml")
    val fs = FileSystem.get(new HConf)
    val xin = fs.open(xpath)
    val xsize = fs.getFileStatus(xpath).getLen
    val xbuf = new Array[Byte](xsize.toInt)
    val xml = XML.load(xin)
    // read parameters
    val instrument = (xml \ "Run" \ "Instrument").text
    val runnum = (xml \ "Run" \ "@Number").text
    val flowcell = (xml \ "Run" \ "Flowcell").text
    toFQ.header = s"@$instrument:$runnum:$flowcell:".getBytes
    val reads = (xml \ "Run" \ "Reads" \ "Read")
      .map(x => ((x \ "@NumCycles").text.toInt, (x \ "@IsIndexedRead").text))
    val fr = reads.map(_._1).scanLeft(1)(_ + _)
    ranges = reads.indices.map(i => (fr(i), fr(i + 1), reads(i)._2))
      .filter(_._3 == "N").map(x => Range(x._1, x._2))
    val lanes = (xml \ "Run" \ "AlignToPhiX" \\ "Lane").map(_.text.toInt)
    // get data from each lane
    lanes.take(1) // TODO :: remove take(1)
      .flatMap(l => readLane(l, fout))
  }
}

object test {
  def main(args: Array[String]) {
    val w = Reader.getAllJobs

    w.map(i => (readBCL.process(i._1), i._2))
      .foreach{ x =>
      (x._1).writeUsingOutputFormat(x._2).setParallelism(1)
    }

    FP.env.execute
  }
}
