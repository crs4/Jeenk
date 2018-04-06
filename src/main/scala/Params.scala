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
  /// required parameters
  var root = param.getRequired("bcl_input_dir")
  if (root.charAt(root.length - 1) != '/') root += "/"
  var fout = param.getRequired("cram_output_dir")
  if (fout.charAt(fout.length - 1) != '/') fout += "/"
  val sref = param.getRequired("reference")
  val stateBE = param.getRequired("flink_tmp_dir")
  /// recommended parameters
  val numnodes = param.getInt("num_nodes", 1)
  val corespernode = param.getInt("cores_per_node", 1)
  val mempernode = param.getInt("mem_per_node", 8000)
  /// other parameters
  val par1 = param.getInt("par1", Math.min(mempernode/8000, corespernode))
  val par2 = param.getInt("par2", Math.min(mempernode/10000, corespernode))
  val meat = numnodes * par2
  val wjobs = Math.min(4, meat / corespernode)
  val kafkaServer = param.get("kafka_server", "127.0.0.1:9092")
  val kafkaTopic = param.get("kafka_prq", "flink-prq")
  val kafkaAligned = param.get("kafka_aligned", "flink-aligned")
  val kafkaControlAL = kafkaAligned + "-con"
  val kafkaControlPRQ = kafkaTopic + "-con"
  val samplePath = param.get("sample_sheet", root + "SampleSheet.csv")
  val adapter = param.get("adapter", null)
  val undet = param.get("undet", "Undetermined")
  val bdir = param.get("base_dir", "Data/Intensities/BaseCalls/")
  val bsize = param.getInt("block_size", 2048)
  val mismatches = param.getInt("mismatches", 1)
  // reader
  val numReaders = param.getInt("num_readers", numnodes)
  val rflinkpar = param.getInt("reader_flinkpar", 1)
  val rgrouping = param.getInt("reader_grouping", par1)
  val rkafkaout = param.getInt("reader_kafka_fanout", 1)
  // aligner
  val numAligners = param.getInt("num_aligners", numnodes)
  val aflinkpar = param.getInt("aligner_flinkpar", par1)
  val agrouping = param.getInt("aligner_grouping", 3)
  val alignerTimeout = param.getInt("aligner_timeout", 0)
  val rapipar = param.getInt("rapi_par", (2*corespernode)/par1)
  val rapiwin = param.getInt("rapi_win", 3360)
  val kafkaparA = param.getInt("aligner_kafka_fanin", 1)
  val kafkaparout = param.getInt("aligner_kafka_fanout", meat / wjobs)
  // writer
  val numWriters = param.getInt("num_writers", wjobs)
  val crampar = Math.min(param.getInt("writer_flinkpar", kafkaparout), kafkaparout)
  val wgrouping = param.getInt("writer_grouping", 4)
  val cramwriterTimeout = param.getInt("writer_timeout", 0)
  val kafkaparW = param.getInt("writer_kafka_fanin", crampar)
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
