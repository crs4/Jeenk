package bclconverter.conf
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.hadoop.conf.{Configuration => HConf}
import org.apache.hadoop.fs.{FileSystem, Path => HPath}
import bclconverter.reader.fuzzyIndex
import Params.{Block, PRQData}

class Params(val param : ParameterTool) extends Serializable {
  var header : Block = Array()
  var ranges : Seq[Seq[Int]] = null
  var index : Seq[Seq[Int]] = null
  var fuz : fuzzyIndex = null
  // general parameters
  val numnodes = param.getInt("numnodes", 1)
  val corespernode = param.getInt("corespernode", 1)
  val mempernode = param.getInt("mempernode", 8000)
  val par1 = param.getInt("par1", Math.min(mempernode/8000, corespernode))
  val adapter = param.get("adapter", null)
  var fout = param.getRequired("fout")
  if (fout.charAt(fout.length - 1) != '/') fout += "/"
  var root = param.getRequired("root")
  if (root.charAt(root.length - 1) != '/') root += "/"
  val samplePath = param.get("sample-sheet", root + "SampleSheet.csv")
  val sref = param.getRequired("reference")
  val stateBE = param.getRequired("stateBE")
  val undet = param.get("undet", "Undetermined")
  val bdir = param.get("bdir", "Data/Intensities/BaseCalls/")
  val bsize = param.getInt("bsize", 2048)
  val mismatches = param.getInt("mismatches", 1)
  val kafkaTopic = param.get("kafkaTopic", "flink-prq")
  val kafkaAligned = param.get("kafkaAligned", "flink-aligned")
  val kafkaControlAL = kafkaAligned + "-con"
  val kafkaControlPRQ = kafkaTopic + "-con"
  // reader
  val numReaders = param.getInt("numReaders", numnodes)
  val rflinkpar = param.getInt("readerflinkpar", 1)
  val rgrouping = param.getInt("rgrouping", par1)
  val rkafkaout = param.getInt("rkafkaout", 1)
  // aligner
  val numAligners = param.getInt("numAligners", numnodes)
  val aflinkpar = param.getInt("alignerflinkpar", par1)
  val agrouping = param.getInt("agrouping", 3)
  val alignerTimeout = param.getInt("alignerTimeout", 0)
  val kafkaparA = param.getInt("akafkain", 1)
  val rapipar = param.getInt("rapipar", (2*corespernode)/par1)
  val rapiwin = param.getInt("rapiwin", 3360)
  val par2 = param.getInt("par2", Math.min(mempernode/10000, corespernode))
  val meat = numnodes * par2
  val wjobs = Math.min(4, meat / corespernode)
  val kafkaparout = param.getInt("akafkaout", meat / wjobs)
  // writer
  val numWriters = param.getInt("numWriters", wjobs)
  val crampar = Math.min(param.getInt("crampar", kafkaparout), kafkaparout)
  val wgrouping = param.getInt("wgrouping", 4)
  val cramwriterTimeout = param.getInt("cramwriterTimeout", 0)
  val kafkaServer = param.get("kafkaServer", "127.0.0.1:9092")
  val kafkaparW = param.getInt("wkafkain", crampar)
}

object Params {
  type Block = Array[Byte]
  type PRQData = (Block, Block, Block, Block, Block)
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
