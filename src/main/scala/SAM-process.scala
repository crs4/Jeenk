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

package bclconverter.sam

import bclconverter.conf.Params.{Block, PRQData}
import com.typesafe.config.ConfigFactory
import htsjdk.samtools.cram.ref.ReferenceSource
import htsjdk.samtools.{SAMProgramRecord, SAMRecord, CigarOperator, Cigar, CigarElement, SAMFileHeader, SAMSequenceRecord, MyCRAMContainerStreamWriter}
import htsjdk.samtools.{SAMRecord, BAMRecordCodec}
import it.crs4.rapi.{Alignment, AlignOp, Contig, Read, Fragment, Batch, Ref, AlignerState, Rapi, RapiUtils, RapiConstants, Opts}
import java.io.{DataOutput, DataInput}
// import org.apache.flink.api.java.hadoop.mapreduce.utils.HadoopUtils
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.extensions._
import org.apache.flink.streaming.api.scala.function.{WindowFunction, AllWindowFunction, RichWindowFunction, RichAllWindowFunction}
import org.apache.flink.streaming.api.windowing.windows.{Window, GlobalWindow, TimeWindow}
import org.apache.flink.streaming.connectors.fs.{Writer => FWriter, StreamWriterBase}
import org.apache.flink.util.Collector
import org.apache.hadoop.conf.{Configuration => HConf}
import org.apache.hadoop.fs.{FileSystem, Path => HPath}
import org.seqdoop.hadoop_bam.util.{DataInputWrapper, DataOutputWrapper}
import org.seqdoop.hadoop_bam.util.{NIOFileUtil, SAMHeaderReader}
import org.seqdoop.hadoop_bam.{SAMFormat, AnySAMInputFormat, SAMRecordWritable, CRAMInputFormat, KeyIgnoringCRAMOutputFormat, KeyIgnoringCRAMRecordWriter, KeyIgnoringBAMOutputFormat, KeyIgnoringBAMRecordWriter, KeyIgnoringAnySAMOutputFormat, CRAMRecordWriter, LazyBAMRecordFactory}
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._
import scala.collection.mutable

object CRAMWriter {
  val refs = mutable.Map[String, ReferenceSource]()
  def getRefSource(r : String) : ReferenceSource = {
    refs.getOrElseUpdate(r, new ReferenceSource(NIOFileUtil.asPath(r)))
  }
}

// Writer from SAMRecord to CRAM format
class CRAMWriter(var ref : String) extends StreamWriterBase[SAMRecord] {
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
  def write(rec : SAMRecord) = {
    // add back header
    rec.setHeader(header)
    cramContainerStream.writeAlignment(rec)
  }
  override
  def open(fs : FileSystem, path : HPath) = {
    header = RapiAligner.getHeader(ref)
    refSource = CRAMWriter.getRefSource(ref)
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
  private def readObject(in : java.io.ObjectInputStream) {
    rapipar = in.readInt
    RapiAligner.synchronized { init }
  }
  def init = {
    setShareRefMem(true)
    setNThreads(rapipar)
    Rapi.init(this)
  }
  init
}

object RapiAligner {
  synchronized {
    RapiUtils.loadPlugin
  }
  private var rp = 1
  private var opts = new MyOpts(rp)
  def getOpts(rapipar : Int) : MyOpts = {
    if (rapipar != rp) {
      opts = new MyOpts(rapipar)
      rp = rapipar
    }
    opts
  }
  def createSamHeader(rapiRef : Ref) : SAMFileHeader =  {
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
  val refs = mutable.Map[String, MyRef]()
  val headers = mutable.Map[String, SAMFileHeader]()
  def getRef(r : String) : MyRef = {
    refs.getOrElseUpdate(r, new MyRef(r))
  }
  def getHeader(r : String) : SAMFileHeader = {
    headers.getOrElseUpdate(r, createSamHeader(getRef(r)))
  }
}


class RapiAligner(var rapipar : Int) extends Serializable {
  private def writeObject(out : java.io.ObjectOutputStream) {
    // out.writeObject(r)
    out.writeInt(rapipar)
  }
  private def readObject(in : java.io.ObjectInputStream){
    // r = in.readObject.asInstanceOf[String]
    rapipar = in.readInt
    init
  }
  def init = RapiAligner.synchronized {
    aligner = new AlignerState(RapiAligner.getOpts(rapipar))
  }
  // start here
  var aligner : AlignerState = null
}

object PRQAligner {
  val logger = LoggerFactory.getLogger("rootLogger")
}

class PRQAligner[W <: Window, Key](refPath : String, rapipar : Int) extends RichWindowFunction[(Key, PRQData), SAMRecord, Key, W] {
  def doJob(in : Iterable[PRQData], out : Collector[SAMRecord]) = RapiAligner.synchronized {
    def toRec2(in : Iterable[Read]) : Iterable[SAMRecord] = {
      def toRec(read : Read, readNum : Int, mate : Read) : SAMRecord = {
        // some functions
        def cigarRapiToHts(cigars : Seq[AlignOp]) : Cigar = {
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
          new Cigar(cigars.map(alignOpToCigarElement))
        }
        // toRec starts here
        if (readNum < 1 || readNum > 2)
          throw new IllegalArgumentException(s"readNum $readNum is out of bounds -- only 1 and 2 are supported")

        val out = new SAMRecord(RapiAligner.getHeader(refPath))

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
          out.setSecondaryAlignment(false)
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
        // remove header, it will be adde back later
        out.setHeader(null)
        out
      }
      // teRec2 starts here
      val in2 = in.toArray
      val r = in2.head
      val m = in2.last
      List(toRec(r, 1, m), toRec(m, 2, r))
    }
    // doJob starts here
    var reads : Batch = null
    val chr = java.nio.charset.Charset.forName("US-ASCII")
    reads = new Batch(2)
    reads.reserve(in.size << 1)
    in.foreach{ x =>
      val (h, b1, q1, b2, q2) = x
      reads.append(new String(h, chr), new String(b1, chr), new String(q1, chr), RapiConstants.QENC_SANGER)
      reads.append(new String(h, chr), new String(b2, chr), new String(q2, chr), RapiConstants.QENC_SANGER)
    }
    // align and get SAMRecord's
    val mapal = ali.aligner
    mapal.alignReads(RapiAligner.getRef(refPath), reads)
    val sams = reads.flatMap(p => toRec2(p))
    sams.foreach(x => out.collect(x))
  }
  def apply(k : Key, w : W, in : Iterable[(Key, PRQData)], out : Collector[SAMRecord]) = {
    // PRQAligner.logger.info(s"###### rapiwin: ${in.size}")
    val s = in.map(_._2)
    doJob(s, out)
  }
  override
  def open(conf : Configuration) = {
    // Init
    ali.init
  }
  val ali = new RapiAligner(rapipar)
}
