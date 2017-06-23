package bclconverter

import com.typesafe.config.ConfigFactory
import htsjdk.samtools.{SAMProgramRecord, SAMRecord, CigarOperator, Cigar, CigarElement, SAMFileHeader, SAMSequenceRecord}
import it.crs4.rapi.{Alignment, AlignOp, Contig, Read, Fragment, Batch, Ref, AlignerState, Rapi, RapiUtils, RapiConstants, Opts}
import org.apache.flink.api.common.functions.{MapFunction, FlatMapFunction, ReduceFunction, GroupReduceFunction}
import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.hadoop.mapreduce.{HadoopOutputFormat, HadoopInputFormat}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{WindowFunction, AllWindowFunction}
import org.apache.flink.streaming.api.windowing.windows.{Window, GlobalWindow, TimeWindow}
import org.apache.flink.util.Collector
import org.apache.hadoop.conf.{Configuration => HConf}
import org.apache.hadoop.fs.{FileSystem, FSDataInputStream, FSDataOutputStream, Path => HPath}
import org.apache.hadoop.io.{NullWritable, LongWritable}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat => MapreduceFileInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat => MapreduceFileOutputFormat}
import org.apache.hadoop.mapreduce.{Job, RecordWriter, TaskAttemptContext}
import org.seqdoop.hadoop_bam.util.SAMHeaderReader
import org.seqdoop.hadoop_bam.{AnySAMInputFormat, CRAMInputFormat, SAMRecordWritable, KeyIgnoringCRAMOutputFormat, KeyIgnoringCRAMRecordWriter, KeyIgnoringBAMOutputFormat, KeyIgnoringBAMRecordWriter}
import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Await, Future}

import bclconverter.reader.Reader.{Block, PRQData}

// Writer from SAMRecordWritable to CRAM format
class SAM2CRAM extends KeyIgnoringCRAMOutputFormat[LongWritable] {
  override def getRecordWriter(ctx : TaskAttemptContext) : RecordWriter[LongWritable, SAMRecordWritable] = {
    val conf = ctx.getConfiguration
    readSAMHeaderFrom(head, conf)
    conf.set(CRAMInputFormat.REFERENCE_SOURCE_PATH_PROPERTY, ref)
    super.getRecordWriter(ctx)
  }
  val head = new HPath(roba.header)
  val ref = "file://" + roba.sref
}


class MidAlignerState(opts : Opts) extends AlignerState(opts) {
  def this() {
    this(roba.opts)
  }
}
class MyAlignerState(opts : Opts) extends MidAlignerState(opts) with Serializable {
}
class MidRef(s : String) extends Ref(s) {
  def this() {
    this(roba.sref)
  }
}
class MyRef(s : String) extends MidRef(s) with Serializable {
}
class MyOpts(rapipar : Int) extends Opts with Serializable {
  setShareRefMem(true)
  setNThreads(rapipar)
  Rapi.init(this)
}

object roba {
  RapiUtils.loadPlugin
  val conf = ConfigFactory.load()
  // rapipar
  var rapipar = 1
  val key1 = "rapi.rapipar"
  if (conf.hasPath(key1))
    rapipar = conf.getString(key1).toInt
  val opts = new MyOpts(rapipar)
  // reference
  var sref : String = _
  val key2 = "rapi.sref"
  if (conf.hasPath(key2))
    sref = conf.getString(key2)
  else
    throw new Error("rapi.sref undefined")
  // header
  var header : String = _
  val key3 = "rapi.header"
  if (conf.hasPath(key3))
    sref = conf.getString(key3)
  else
    throw new Error("rapi.header undefined")
}

class SomeData(r : String, rapipar : Int) extends Serializable {
  def createSamHeader(rapiRef : Ref) : SAMFileHeader = {
    def convertContig(rapiContig : Contig) : SAMSequenceRecord = {
      val sr = new SAMSequenceRecord(rapiContig.getName, rapiContig.getLen.toInt)
      if (rapiContig.getAssemblyIdentifier != null)
        sr.setAssembly(rapiContig.getAssemblyIdentifier)
      if (rapiContig.getSpecies != null)
        sr.setSpecies(rapiContig.getSpecies)
      if (rapiContig.getMd5 != null)
        sr.setMd5(rapiContig.getMd5)
      sr
    }
    def rapiVerStr = s"Rapi plugin - aligner: ${Rapi.getAlignerName}; aligner version: ${Rapi.getAlignerVersion}; plugin version: ${Rapi.getPluginVersion}"
    val newHeader = new SAMFileHeader()
    rapiRef.foreach(contig => newHeader.addSequence(convertContig(contig)))
    newHeader.addProgramRecord(new SAMProgramRecord("Myname ver x.y.z with " + rapiVerStr))
    newHeader
  }
  def init = {
    RapiUtils.loadPlugin
    val opts = new MyOpts(rapipar)
    mapAligner = Stream.continually(new MyAlignerState(opts))
    ref = new MyRef(r)
    header = createSamHeader(ref)
  }
  // start here
  var ref : MyRef = _
  var header : SAMFileHeader = _
  var mapAligner : Stream[MyAlignerState] = _
}

class PRQ2SAMRecord[W <: Window](refPath : String) extends WindowFunction[(Int, PRQData), (Int, SAMRecordWritable), Tuple, W] {
  //with AllWindowFunction[(Int, PRQData), (Int, SAMRecordWritable), W] {
  def alignOpToCigarElement(alnOp : AlignOp) : CigarElement = {
    val cigarOp = (alnOp.getType) match {
      case AlignOp.Type.Match => CigarOperator.M
      case AlignOp.Type.Insert => CigarOperator.I
      case AlignOp.Type.Delete => CigarOperator.D
      case AlignOp.Type.SoftClip => CigarOperator.S
      case AlignOp.Type.HardClip => CigarOperator.H
      case AlignOp.Type.Skip => CigarOperator.N
      case AlignOp.Type.Pad => CigarOperator.P
      case what => throw new IllegalArgumentException("Unexpected align operation " + what)
    }
    new CigarElement(alnOp.getLen, cigarOp)
  }
  def cigarRapiToHts(cigars : Seq[AlignOp]) : Cigar = {
    new Cigar(cigars.map(alignOpToCigarElement))
  }
  def toRec2(in : Iterable[Read]) : Iterable[SAMRecordWritable] = {
    val in2 = in.toArray
    val r = in2.head
    val m = in2.last

    List(toRec(r, 1, m), toRec(m, 2, r))
  }
  def toRec(read : Read, readNum : Int, mate : Read) : SAMRecordWritable = {
    if (readNum < 1 || readNum > 2)
      throw new IllegalArgumentException(s"readNum $readNum is out of bounds -- only 1 and 2 are supported")

    val out = new SAMRecord(dati.header)

    val b = read.getSeq
    val q = read.getQual
    out.setReadString(b)
    out.setBaseQualityString(q)

    out.setReadFailsVendorQualityCheckFlag(false)
    out.setReadName(read.getId)
    
    out.setReadPairedFlag(mate != null)
    out.setFirstOfPairFlag(readNum == 1)
    out.setSecondOfPairFlag(readNum == 2)

    if (read.getMapped()) {
      val aln : Alignment = read.getAln(0)
      out.setReadUnmappedFlag(false)
      out.setAlignmentStart(aln.getPos)
      out.setCigar(cigarRapiToHts(aln.getCigarOps))
      out.setMappingQuality(aln.getMapq)
      out.setNotPrimaryAlignmentFlag(false)
      out.setSupplementaryAlignmentFlag(aln.getSecondaryAln)
      out.setProperPairFlag(read.getPropPaired)
      out.setReadNegativeStrandFlag(aln.getReverseStrand)
      out.setReferenceName(aln.getContig.getName)

      if (mate != null) {
        if (mate.getMapped()) {
          val mateAln : Alignment = mate.getAln(0)
          out.setInferredInsertSize(Rapi.getInsertSize(aln, mateAln))
          out.setMateAlignmentStart(mateAln.getPos)
          out.setMateNegativeStrandFlag(mateAln.getReverseStrand)
          out.setMateReferenceName(mateAln.getContig.getName)
          out.setMateUnmappedFlag(false)
        }
        else
          out.setMateUnmappedFlag(false)
      }
      // tags
      out.setAttribute("NM", new Integer(aln.getNMismatches))
      out.setAttribute("AS", new Integer(aln.getScore))
      for (entry <- aln.getTags.entrySet)
        out.setAttribute(entry.getKey, entry.getValue)
    }
    else {
      out.setReadUnmappedFlag(false)
    }
    val r = new SAMRecordWritable
    r.set(out)
    r
  }
  def doJob(fn : Int, in : Iterable[(Int, PRQData)], out : Collector[(Int, SAMRecordWritable)]) = {
    // insert PRQ data
    val reads = new Batch(2)
    val chr = java.nio.charset.Charset.forName("US-ASCII")
    reads.reserve(in.size << 1)
    in.map(_._2)
      .foreach{ x =>
      val (h, b1, q1, b2, q2) = x
      reads.append(new String(h, chr), new String(b1, chr), new String(q1, chr), RapiConstants.QENC_SANGER)
      reads.append(new String(h, chr), new String(b2, chr), new String(q2, chr), RapiConstants.QENC_SANGER)
    }
    // align and get SAMRecord's
    val mapal = dati.mapAligner(fn)
    mapal.alignReads(dati.ref, reads)
    val sams = reads.flatMap(p => toRec2(p))
    println(s"#### reads:${reads.size}")
    sams.foreach(x => out.collect((fn, x)))
  }
  def apply(key : Tuple, w : W, in : Iterable[(Int, PRQData)], out : Collector[(Int, SAMRecordWritable)]) = {
    // insert PRQ data
    val fn : Int = key.getField(0)
    doJob(fn, in, out)
  }
  // Init
  var dati = new SomeData(refPath, roba.rapipar)
  dati.init
}
