package bclconverter.bamcram

import bclconverter.Fenv
import bclconverter.bclreader.Reader.{Block, MyFS}
import it.crs4.rapi.{Alignment, AlignOp, Contig, Read, Fragment, Batch, Ref, AlignerState, Rapi, RapiUtils, RapiConstants, Opts}
import org.apache.flink.api.common.functions.{MapFunction, FlatMapFunction, ReduceFunction, GroupReduceFunction}
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.{Window, GlobalWindow, TimeWindow}
import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.hadoop.mapreduce.{HadoopOutputFormat, HadoopInputFormat}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._
import org.apache.hadoop.conf.{Configuration => HConf}
import org.apache.hadoop.fs.{FileSystem, FSDataInputStream, FSDataOutputStream, Path => HPath}
import org.apache.hadoop.io.{NullWritable, LongWritable}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat => MapreduceFileInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat => MapreduceFileOutputFormat}
import org.apache.hadoop.mapreduce.{Job, RecordWriter, TaskAttemptContext}
import org.seqdoop.hadoop_bam.util.SAMHeaderReader
import org.seqdoop.hadoop_bam.{AnySAMInputFormat, CRAMInputFormat, SAMRecordWritable, KeyIgnoringCRAMOutputFormat,
  KeyIgnoringCRAMRecordWriter, KeyIgnoringBAMOutputFormat, KeyIgnoringBAMRecordWriter}

import htsjdk.samtools.{SAMProgramRecord, SAMRecord, CigarOperator, Cigar, CigarElement, SAMFileHeader, SAMSequenceRecord}
import scala.collection.JavaConversions._

import Varia.PRQData
object Varia {
  type PRQData = (Block, Block, Block, Block, Block)
}

/*
class CramOF(filename : String) extends OutputFormat[PRQData] {
  var out : FSDataOutputStream = null
  def close() = {
    out.close
  }
  def configure(conf : Configuration) = {
  }
  def open(taskNumber : Int, numTasks : Int) = {
    val path = new HPath(filename)
    val fs = MyFS(path)
    if (fs.exists(path)) 
      fs.delete(path, true)
    out = fs.create(path)
  }
  def writeRecord(rec : PRQData) = {
    out.write("ciao\n".getBytes)
  }
}
 */

object roba {
  RapiUtils.loadPlugin()
  val ref = "/u/cesco/dump/data/bam/c/chr1.fasta"
  val opts = new MyOpts
  opts.setShareRefMem(true)
  // opts.setNThreads(1)
  Rapi.init(opts)
  def getRef : MyRef = {
    new MyRef(ref)
  }
}

class MyRefMid(s : String) extends Ref(s) {
  def this() {
    this(roba.ref)
  }
}
class MyRef(s : String) extends MyRefMid(s) with Serializable {
}
class MyOpts extends Opts with Serializable {}
class MyAlignerStateMid(opts : Opts) extends AlignerState(opts) {
  def this() {
    this(roba.opts)
  }
}
class MyAlignerState(opts : Opts) extends MyAlignerStateMid(opts) with Serializable {}

class PRQ2SAMRecord(refPath : String) extends AllWindowFunction[PRQData, SAMRecordWritable, GlobalWindow] {
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

    val out = new SAMRecord(header)

    val b = read.getSeq
    val q = read.getQual
    ///// TOGLIMI
    if (b.size != q.size)
      println(s"XXXXXXX ${b.size} ${q.size}")
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

  def apply(w : GlobalWindow, in : Iterable[PRQData], out : Collector[SAMRecordWritable]) = {
    // insert PRQ data
    RapiUtils.loadPlugin()
    val reads = new Batch(2)
    val chr = java.nio.charset.Charset.forName("US-ASCII")
    reads.reserve(in.size << 1)
    in.foreach{ x =>
      val (h, b1, q1, b2, q2) = x
      reads.append(new String(h, chr), new String(b1, chr), new String(q1, chr), RapiConstants.QENC_SANGER)
      reads.append(new String(h, chr), new String(b2, chr), new String(q2, chr), RapiConstants.QENC_SANGER)
    }
    // align and get SAMRecord's
    aligner.alignReads(ref, reads)
    reads
      .flatMap(p => toRec2(p))
      .foreach(x => out.collect(x))
  }
  // Init
  Rapi.init(roba.opts)
  val ref = new MyRef(refPath)
  val header = createSamHeader(ref)
  val aligner = new MyAlignerState(roba.opts)
}


////////////////////////////////////////////////////////////////////////////////

class Prova {
}

////////////////////////////////////////////////////////////////////////////////

class SAM2CRAM extends KeyIgnoringCRAMOutputFormat[LongWritable] with Serializable {
  override def getRecordWriter(ctx : TaskAttemptContext) : RecordWriter[LongWritable, SAMRecordWritable] = {
    val conf = ctx.getConfiguration
    readSAMHeaderFrom(head, conf)
    conf.set(CRAMInputFormat.REFERENCE_SOURCE_PATH_PROPERTY, ref)
    super.getRecordWriter(ctx)
  }
  val head = new HPath("file:///u/cesco/dump/data/bam/coso.bam")
  val ref = "file:///u/cesco/dump/data/bam/c/chr1.fasta"
}

/*

class PRQ2SAM(refPath : String) extends AllWindowFunction[PRQData, String, GlobalWindow] {
  // init
  RapiUtils.loadPlugin()
  val opts = new MyOpts
  opts.setShareRefMem(true)
  // opts.setNThreads(8)
  Rapi.init(opts)
  val aligner = new MyAlignerState(opts)

  def apply(w : GlobalWindow, in : Iterable[PRQData], out : Collector[String]) = {
    // other stuff
    val ref = new Ref(refPath)
    val reads = new Batch(2)
    reads.reserve(in.size)
    in.foreach{ x =>
      val (h, b1, q1, b2, q2) = x
      reads.append(new String(h), new String(b1), new String(q1), RapiConstants.QENC_SANGER)
      reads.append(new String(h), new String(b2), new String(q2), RapiConstants.QENC_SANGER)
    }
    aligner.alignReads(ref, reads)
    out.collect(Rapi.formatSamBatch(reads))
  }
}



class MyOF extends KeyIgnoringCRAMOutputFormat[LongWritable] with Serializable {
  override def getRecordWriter(ctx : TaskAttemptContext) : RecordWriter[LongWritable, SAMRecordWritable] = {
    val conf = ctx.getConfiguration
    readSAMHeaderFrom(head, conf)
    conf.set(CRAMInputFormat.REFERENCE_SOURCE_PATH_PROPERTY, ref)
    println("******************** Getting RecordWriter ********************")
    super.getRecordWriter(ctx)
  }
  val head = new HPath("file:///u/cesco/dump/data/bam/coso.bam")
  val ref = "file:///u/cesco/dump/data/bam/c/chr1.fasta" //"file:///u/cesco/dump/data/bam/ref.fa"
}

object test {
  def doStuff() = {
    val propertiesFile = "conf/bclconverter.properties"
    val param = ParameterTool.fromPropertiesFile(propertiesFile)
    var flinkpar = 1
    flinkpar = param.getInt("flinkpar", flinkpar)
    val env = (new Fenv).env
    env.setParallelism(flinkpar)
    // cose
    val ipath = new HPath("file:///u/cesco/dump/data/bam/coso.bam")
    val opath = new HPath("file:///u/cesco/dump/data/bam/coso.cram")
    lazy val conf = new HConf
    val job = Job.getInstance(conf)
    val hif = new HadoopInputFormat(new AnySAMInputFormat, classOf[LongWritable], classOf[SAMRecordWritable], job)
    MapreduceFileInputFormat.addInputPath(job, ipath)
    val bam = env.createInput(hif)
    MapreduceFileOutputFormat.setOutputPath(job, opath)
    val hof = new HadoopOutputFormat(new MyOF, job)
    bam.writeUsingOutputFormat(hof)
    env.execute
    hof.finalizeGlobal(flinkpar)
  }
  def main(args: Array[String]) {
    doStuff
  }
}

 */
