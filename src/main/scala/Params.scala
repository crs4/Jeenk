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

package bclconverter.conf
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.hadoop.conf.{Configuration => HConf}
import org.apache.hadoop.fs.{FileSystem, Path => HPath}
import bclconverter.reader.fuzzyIndex
import scala.util.parsing.json.JSON
import Params.{Block, PRQData}

class Params(val param : ParameterTool) extends Serializable {
  var header : Block = Array()
  var ranges : Seq[Seq[Int]] = null
  var index : Seq[Seq[Int]] = null
  var fuz : fuzzyIndex = null
  /// required parameters
  var root = param.getRequired("bcl_input_dir")
  if (!root.endsWith("/")) root += "/"
  var fout = param.getRequired("cram_output_dir")
  if (!fout.endsWith("/")) fout += "/"
  val sref = param.getRequired("reference")
  val stateBE = param.getRequired("flink_tmp_dir")
  // parameters of the flink cluster
  val flserv = param.get("flink_server", "localhost:8081")
  val url = s"http://${flserv}/overview"
  val raw = scala.io.Source.fromURL(url).mkString
  val mpar = JSON.parseFull(raw).get.asInstanceOf[Map[String, Any]]
  val slots = mpar("slots-total").asInstanceOf[Double].toInt
  val tasksmanagers = mpar("taskmanagers").asInstanceOf[Double].toInt
  /// recommended parameters
  val minmem = 12000
  val numnodes = param.getInt("num_nodes", tasksmanagers)
  val maxpar = param.getInt("cores_per_node", slots/tasksmanagers)
  val mempernode = param.getInt("mem_per_node", minmem)
  /// other parameters
  val par1 = param.getInt("par1", Math.min(mempernode/8000, maxpar))
  val par2 = param.getInt("par2", Math.min(mempernode/minmem, maxpar))
  val kafkaServer = param.get("kafka_server", "127.0.0.1:9092")
  val kafkaTopic = param.get("kafka_prq", "flink-prq")
  val kafkaAligned = param.get("kafka_aligned", "flink-aligned")
  val kafkaControlAL = kafkaAligned + "-con"
  val kafkaControlPRQ = kafkaTopic + "-con"
  val samplePath = param.get("sample_sheet", root + "SampleSheet.csv")
  val adapter = param.get("adapter", null)
  val undet = param.get("undet", "Undetermined")
  var bdir = param.get("base_dir", "Data/Intensities/BaseCalls/")
  if (!bdir.endsWith("/")) bdir += "/"
  val bsize = param.getInt("block_size", 2048)
  val mismatches = param.getInt("mismatches", 1)
  // reader
  val numReaders = param.getInt("num_readers", numnodes)
  val rflinkpar = param.getInt("reader_flinkpar", 1)
  val rgrouping = param.getInt("reader_grouping", par1)
  val rkafkaout = param.getInt("reader_kafka_fanout", 1)
  // aligner
  val numAligners = param.getInt("num_aligners", numnodes)
  val aflinkpar = param.getInt("aligner_flinkpar", 1)
  val agrouping = param.getInt("aligner_grouping", 3)
  val alignerTimeout = param.getInt("aligner_timeout", 0)
  val rapipar = param.getInt("rapi_par", maxpar / aflinkpar)
  val rapiwin = param.getInt("rapi_win", 3360)
  val kafkaparA = param.getInt("aligner_kafka_fanin", 1)
  val meat = numnodes * par2
  val wjobs = Math.min(4, numnodes)
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
