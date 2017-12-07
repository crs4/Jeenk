package bclconverter.aligner

import bclconverter.reader.Reader.{Block, PRQData}
import com.typesafe.config.ConfigFactory
import htsjdk.samtools.cram.ref.ReferenceSource
import htsjdk.samtools.{SAMProgramRecord, SAMRecord, CigarOperator, Cigar, CigarElement, SAMFileHeader, SAMSequenceRecord, MyCRAMContainerStreamWriter}
import htsjdk.samtools.{SAMRecord, BAMRecordCodec}
import it.crs4.rapi.{Alignment, AlignOp, Contig, Read, Fragment, Batch, Ref, AlignerState, Rapi, RapiUtils, RapiConstants, Opts}
import java.io.{DataOutput, DataInput}
import org.apache.flink.api.java.hadoop.mapreduce.utils.HadoopUtils
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{WindowFunction, AllWindowFunction, RichWindowFunction, RichAllWindowFunction}
import org.apache.flink.streaming.api.windowing.windows.{Window, GlobalWindow, TimeWindow}
import org.apache.flink.streaming.connectors.fs.{Writer => FWriter, StreamWriterBase}
import org.apache.flink.util.Collector
import org.apache.hadoop.conf.{Configuration => HConf}
import org.apache.hadoop.fs.{FileSystem, Path => HPath}
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.{NullWritable, LongWritable}
import org.apache.hadoop.mapreduce.{Job, JobID, RecordWriter, TaskAttemptContext, TaskAttemptID, OutputCommitter}
import org.seqdoop.hadoop_bam.util.{DataInputWrapper, DataOutputWrapper}
import org.seqdoop.hadoop_bam.util.{NIOFileUtil, SAMHeaderReader}
import org.seqdoop.hadoop_bam.{SAMFormat, AnySAMInputFormat, SAMRecordWritable, CRAMInputFormat, KeyIgnoringCRAMOutputFormat, KeyIgnoringCRAMRecordWriter, KeyIgnoringBAMOutputFormat, KeyIgnoringBAMRecordWriter, KeyIgnoringAnySAMOutputFormat, CRAMRecordWriter, LazyBAMRecordFactory}
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._


object MySAMRecordWritable {
  val lazyCodec = new BAMRecordCodec(null, new LazyBAMRecordFactory)
}

class MySAMRecordWritable extends Writable {
  private var record : SAMRecord = _
  def get = record
  def set(r : SAMRecord) = {
    record = r
  }
  override
  def write(out : DataOutput) =  {
    val codec = new BAMRecordCodec(record.getHeader)
    codec.setOutputStream(new DataOutputWrapper(out))
    codec.encode(record)
  }
  override
  def readFields(in : DataInput) = {
    MySAMRecordWritable.lazyCodec.setInputStream(new DataInputWrapper(in))
    record = MySAMRecordWritable.lazyCodec.decode
  }
  def reset(head : SAMFileHeader) {
    val pin = new java.io.PipedInputStream
    val pout = new java.io.PipedOutputStream(pin)
    val codecw = new BAMRecordCodec(head)
    codecw.setOutputStream(pout)
    codecw.encode(record)
    val codecr = new BAMRecordCodec(head)
    codecr.setInputStream(pin)
    record = codecr.decode
  }
}


// Writer from SAMRecord to CRAM format
class CRAMWriter(var ref : String) extends StreamWriterBase[(LongWritable, SAMRecord)] {
  // Serialization
  private def writeObject(out : java.io.ObjectOutputStream) = {
    out.writeObject(ref)
  }
  private def readObject(in : java.io.ObjectInputStream) = {
    ref = in.readObject.asInstanceOf[String]
  }
  override
  def duplicate() : CRAMWriter = {
    new CRAMWriter(ref)
  }
  override
  def write(record: (LongWritable, SAMRecord)) = {
    val rec = record._2
    // myr.reset(header)
    // val rec = myr.get
    cramContainerStream.writeAlignment(rec)
  }
  def createHeader = {
    val conf = HadoopFileSystem.getHadoopConfiguration
    val sd = new SomeData(ref, 1)
    sd.init
    header = SomeData.createSamHeader(sd.ref)
    sd.close
    refSource = new ReferenceSource(NIOFileUtil.asPath(ref))
  }
  override
  def open(fs : FileSystem, path : HPath) = {
    if (header == null || refSource == null)
      createHeader
    super.open(fs, path)
    cramContainerStream = new MyCRAMContainerStreamWriter(getStream, null, refSource, header, HADOOP_BAM_PART_ID)
    cramContainerStream.writeHeader(header)
  }
  override
  def close = {
    cramContainerStream.finish(false)
    cramContainerStream = null
    refSource = null
    super.close
  }
  // start
  val HADOOP_BAM_PART_ID = "Hadoop-BAM-Part"
  var refSource : ReferenceSource = null
  var header : SAMFileHeader = null
  var cramContainerStream : MyCRAMContainerStreamWriter = null
}

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
  def init =  synchronized {
    setShareRefMem(true)
    setNThreads(rapipar)
    Rapi.init(this)
  }
  init
}

object SomeData {
  synchronized {
    RapiUtils.loadPlugin
  }
  private var opts : MyOpts = null
  def getOpts(rapipar : Int) : MyOpts = {
    synchronized {
      if (opts == null) {
        opts = new MyOpts(rapipar)
      }
    }
    opts
  }
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
    synchronized {
      rapiRef.foreach(contig => newHeader.addSequence(convertContig(contig)))
      newHeader.addProgramRecord(new SAMProgramRecord("Myname ver x.y.z with " + rapiVerStr))
    }
    newHeader
  }
}

class SomeData(var r : String, var rapipar : Int) extends Serializable {
  private def writeObject(out : java.io.ObjectOutputStream) {
    out.writeObject(r)
    out.writeInt(rapipar)
  }
  private def readObject(in : java.io.ObjectInputStream){
    r = in.readObject.asInstanceOf[String]
    rapipar = in.readInt
    init
  }
  def init = synchronized {
    aligner = new AlignerState(SomeData.getOpts(rapipar))
    ref = new MyRef(r)
    header = SomeData.createSamHeader(ref)
  }
  def close = synchronized {
    ref.unload
  }
  // start here
  var ref : MyRef = _
  var header : SAMFileHeader = _
  var aligner : AlignerState = _
}


class PRQAligner[W <: Window](refPath : String, rapipar : Int) extends RichWindowFunction[(Int, PRQData), SAMRecord, Tuple, W] {
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
  def toRec2(in : Iterable[Read]) : Iterable[SAMRecord] = {
    val in2 = in.toArray
    val r = in2.head
    val m = in2.last

    List(toRec(r, 1, m), toRec(m, 2, r))
  }
  def toRec(read : Read, readNum : Int, mate : Read) : SAMRecord = synchronized {
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
    // val r = new SAMRecordWritable
    // r.set(out)
    // r
    out
  }
  def doJob(in : Iterable[PRQData], out : Collector[SAMRecord]) = {
    // insert PRQ data
    var reads : Batch = null
    val chr = java.nio.charset.Charset.forName("US-ASCII")
    synchronized {
      reads = new Batch(2)
      reads.reserve(in.size << 1)
    }
    in.foreach{ x =>
      val (h, b1, q1, b2, q2) = x
      synchronized {
        reads.append(new String(h, chr), new String(b1, chr), new String(q1, chr), RapiConstants.QENC_SANGER)
        reads.append(new String(h, chr), new String(b2, chr), new String(q2, chr), RapiConstants.QENC_SANGER)
      }
    }
    // align and get SAMRecord's
    val mapal = dati.aligner
    synchronized { mapal.alignReads(dati.ref, reads) }
    val sams = reads.flatMap(p => toRec2(p))
    sams.foreach(x => out.collect(x))
  }
  def apply(key : Tuple, w : W, in : Iterable[(Int, PRQData)], out : Collector[SAMRecord]) = {
    // insert PRQ data
    val s = in.map(_._2)
    doJob(s, out)
  }
  override
  def open(conf : Configuration) = {
    // Init
    dati.init
  }
  override
  def close = {
    // force munmapping of reference
    dati.close
  }
  val dati = new SomeData(refPath, rapipar)
}

