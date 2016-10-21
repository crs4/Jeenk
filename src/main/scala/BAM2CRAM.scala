package bclconverter.bamcram

import bclconverter.Fenv
import bclconverter.bclreader.Reader.{Block, MyFS}
import it.crs4.rapi.{Read, Fragment, Batch, Ref, AlignerState, Rapi, RapiUtils, RapiConstants, Opts}
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

import htsjdk.samtools.SAMRecord

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
  val ref = "/home/cesco/dump/data/bam/c/chr1.fasta"
  val opts = new MyOpts
  opts.setShareRefMem(true)
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
  // init
  // RapiUtils.loadPlugin()
  // val opts = new MyOpts
  // opts.setShareRefMem(true)
  // opts.setNThreads(1)
  Rapi.init(roba.opts)
  val ref = new MyRef(refPath)
  val aligner = new MyAlignerState(roba.opts)

  def toRec(in : Read) : SAMRecordWritable = {
    val out = new SAMRecord(null)
    out.setReadName(in.getId)
    out.setReadBases(in.getSeq.getBytes)
    out.setBaseQualities(in.getQual.getBytes)
    val r = new SAMRecordWritable
    r.set(out)
    r
  }

  def apply(w : GlobalWindow, in : Iterable[PRQData], out : Collector[SAMRecordWritable]) = {
    // insert PRQ data
    RapiUtils.loadPlugin()
    val reads = new Batch(2)
    reads.reserve(in.size)
    in.foreach{ x =>
      val (h, b1, q1, b2, q2) = x
      reads.append(new String(h), new String(b1), new String(q1), RapiConstants.QENC_SANGER)
      reads.append(new String(h), new String(b2), new String(q2), RapiConstants.QENC_SANGER)
    }
    // align and get SAMRecord's
    aligner.alignReads(ref, reads)
    val bit = reads.iterator
    while(bit.hasNext) {
      val fit = bit.next.iterator
      while(fit.hasNext)
        out.collect(toRec(fit.next))
    }
  }
}

class SAM2CRAM extends KeyIgnoringCRAMOutputFormat[LongWritable] with Serializable {
  override def getRecordWriter(ctx : TaskAttemptContext) : RecordWriter[LongWritable, SAMRecordWritable] = {
    val conf = ctx.getConfiguration
    readSAMHeaderFrom(head, conf)
    conf.set(CRAMInputFormat.REFERENCE_SOURCE_PATH_PROPERTY, ref)
    super.getRecordWriter(ctx)
  }
  val head = new HPath("file:///home/cesco/dump/data/bam/coso.bam")
  val ref = "file:///home/cesco/dump/data/bam/c/chr1.fasta"
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
  val head = new HPath("file:///home/cesco/dump/data/bam/coso.bam")
  val ref = "file:///home/cesco/dump/data/bam/c/chr1.fasta" //"file:///home/cesco/dump/data/bam/ref.fa"
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
    val ipath = new HPath("file:///home/cesco/dump/data/bam/coso.bam")
    val opath = new HPath("file:///home/cesco/dump/data/bam/coso.cram")
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
