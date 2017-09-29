package bclconverter.aligner

import com.typesafe.config.ConfigFactory
import htsjdk.samtools.{SAMProgramRecord, SAMRecord, CigarOperator, Cigar, CigarElement, SAMFileHeader, SAMSequenceRecord}
import it.crs4.rapi.{Alignment, AlignOp, Contig, Read, Fragment, Batch, Ref, AlignerState, Rapi, RapiUtils, RapiConstants, Opts}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala.hadoop.mapreduce.HadoopOutputFormat
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{WindowFunction, AllWindowFunction}
import org.apache.flink.streaming.api.windowing.windows.{Window, GlobalWindow, TimeWindow}
import org.apache.flink.util.Collector
import org.apache.hadoop.conf.{Configuration => HConf}
import org.apache.hadoop.fs.{Path => HPath}
import org.apache.hadoop.io.{NullWritable, LongWritable}
import org.apache.hadoop.mapreduce.{Job, RecordWriter, TaskAttemptContext}
import org.seqdoop.hadoop_bam.SAMFormat
import org.seqdoop.hadoop_bam.{AnySAMInputFormat, CRAMInputFormat, SAMRecordWritable, KeyIgnoringCRAMOutputFormat, KeyIgnoringCRAMRecordWriter, KeyIgnoringBAMOutputFormat, KeyIgnoringBAMRecordWriter, KeyIgnoringAnySAMOutputFormat}
import scala.collection.JavaConversions._

import bclconverter.reader.Reader.{Block, PRQData}

class MyCRAMOutputFormat(s2c : SAM2CRAM, job : Job) extends HadoopOutputFormat[LongWritable, SAMRecordWritable](s2c, job) {
  private def writeObject(out : java.io.ObjectOutputStream) {
    configuration.write(out)
    out.writeObject(mapreduceOutputFormat)
  }
  private def readObject(in : java.io.ObjectInputStream){
    configuration = new HConf
    configuration.readFields(in)
    mapreduceOutputFormat = in.readObject.asInstanceOf[SAM2CRAM]
  }
}


// Writer from SAMRecordWritable to CRAM format
class SAM2CRAM(var head : String, var ref : String) extends KeyIgnoringCRAMOutputFormat[LongWritable] with Serializable {
  def this() = this(null, null)
  var headpath : HPath = _
  override def getRecordWriter(ctx : TaskAttemptContext, out : HPath) : RecordWriter[LongWritable, SAMRecordWritable] = {
    val conf = ctx.getConfiguration
    readSAMHeaderFrom(headpath, conf)
    setWriteHeader(true)
    conf.set(CRAMInputFormat.REFERENCE_SOURCE_PATH_PROPERTY, ref)
    super.getRecordWriter(ctx, out)
  }
  private def writeObject(out : java.io.ObjectOutputStream) {
    out.writeObject(head)
    out.writeObject(ref)
  }
  private def readObject(in : java.io.ObjectInputStream){
    head = in.readObject.asInstanceOf[String]
    ref = in.readObject.asInstanceOf[String]
    init
  }
  def init {
    if (head != null)
      headpath = new HPath(head)
  }
  // start
  init
}

/************************
class SAM2BAM extends KeyIgnoringBAMOutputFormat[LongWritable] {
  override def getRecordWriter(ctx : TaskAttemptContext, out : HPath) : RecordWriter[LongWritable, SAMRecordWritable] = {
    val conf = ctx.getConfiguration
    readSAMHeaderFrom(myheader, conf)
    setWriteHeader(true)
    super.getRecordWriter(ctx, out)
  }
  val myheader = new HPath(roba.header)
}

class SAM2SAM extends KeyIgnoringAnySAMOutputFormat[LongWritable](SAMFormat.valueOf("SAM")) {
  override def getRecordWriter(ctx : TaskAttemptContext, out : HPath) : RecordWriter[LongWritable, SAMRecordWritable] = {
    val conf = ctx.getConfiguration
    readSAMHeaderFrom(myheader, conf)
    setWriteHeader(true)
    super.getRecordWriter(ctx, out)
  }
  val myheader = new HPath(roba.header)
}
******************************/

class MyRef(var s : String) extends Ref with Serializable {
  private def writeObject(out : java.io.ObjectOutputStream) {
    out.writeObject(s)
  }
  private def readObject(in : java.io.ObjectInputStream){
    s = in.readObject.asInstanceOf[String]
    load(s)
  }
  load(s)
}

class MyOpts(var rapipar : Int) extends Opts with Serializable {
  private def writeObject(out : java.io.ObjectOutputStream) {
    out.writeInt(rapipar)
  }
  private def readObject(in : java.io.ObjectInputStream){
    rapipar = in.readInt
    init
  }
  def init {
    setShareRefMem(true)
    setNThreads(rapipar)
    Rapi.init(this)
  }
  init
}

object rapiStuff {
  RapiUtils.loadPlugin
  def init {}
}

class SomeData(var r : String, var rapipar : Int) extends Serializable {
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
  private def writeObject(out : java.io.ObjectOutputStream) {
    out.writeObject(r)
    out.writeInt(rapipar)
  }
  private def readObject(in : java.io.ObjectInputStream){
    r = in.readObject.asInstanceOf[String]
    rapipar = in.readInt
    init
  }
  def init = {
    RapiUtils.loadPlugin
    val opts = new MyOpts(rapipar)
    aligner = new AlignerState(opts)
    ref = new MyRef(r)
    header = createSamHeader(ref)
  }
  // start here
  var ref : MyRef = _
  var header : SAMFileHeader = _
  var aligner : AlignerState = _
}


class PRQ2SAMRecord[W <: Window](refPath : String, rapipar : Int) extends WindowFunction[(Int, PRQData), SAMRecordWritable, Tuple, W] {
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
          out.setMateUnmappedFlag(true)
      }
      // tags
      out.setAttribute("NM", new Integer(aln.getNMismatches))
      out.setAttribute("AS", new Integer(aln.getScore))
      for (entry <- aln.getTags.entrySet)
        out.setAttribute(entry.getKey, entry.getValue)
    }
    else {
      out.setReadUnmappedFlag(true)
    }
    val r = new SAMRecordWritable
    r.set(out)
    r
  }
  def doJob(in : Iterable[PRQData], out : Collector[SAMRecordWritable]) = {
    // insert PRQ data
    val reads = new Batch(2)
    val chr = java.nio.charset.Charset.forName("US-ASCII")
    reads.reserve(in.size << 1)
    in.foreach{ x =>
      val (h, b1, q1, b2, q2) = x
      reads.append(new String(h, chr), new String(b1, chr), new String(q1, chr), RapiConstants.QENC_SANGER)
      reads.append(new String(h, chr), new String(b2, chr), new String(q2, chr), RapiConstants.QENC_SANGER)
    }
    // align and get SAMRecord's
    val mapal = dati.aligner
    mapal.alignReads(dati.ref, reads)
    val sams = reads.flatMap(p => toRec2(p))
    println(s"#### reads:${reads.size}")
    sams.foreach(x => out.collect(x))
  }
  def apply(key : Tuple, w : W, in : Iterable[(Int, PRQData)], out : Collector[SAMRecordWritable]) = {
    // insert PRQ data
    val s = in.map(_._2)
    doJob(s, out)
  }
  // Init
  rapiStuff.init
  var dati = new SomeData(refPath, rapipar)
  dati.init
}
