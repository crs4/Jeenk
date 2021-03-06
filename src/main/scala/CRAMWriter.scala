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

package it.crs4.jeenk.writer

import com.typesafe.config.ConfigFactory
import htsjdk.samtools.SAMRecord
import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.{Executors, TimeoutException}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.extensions._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers._
import org.apache.flink.streaming.api.windowing.windows.{Window, TimeWindow, GlobalWindow}
import org.apache.flink.streaming.connectors.fs.bucketing.{BucketingSink, BasePathBucketer}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue
import org.apache.flink.streaming.util.serialization.{KeyedSerializationSchema, KeyedDeserializationSchema, DeserializationSchema}
import org.apache.flink.util.MathUtils
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._
import scala.collection.mutable.Map
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Await, Future}
import scala.language.postfixOps

import it.crs4.jeenk.kafka.{MySSerializer, MyPartitioner, ProdProps, ConsProps, MyDeserializer, MySDeserializer}
import it.crs4.jeenk.conf.Params
import it.crs4.jeenk.conf.Params.Block
import it.crs4.jeenk.aligner.MyWaterMarker

class miniWriter(pl : Params, ind : (Int, Int)) {
  // initialize stream environment
  var env = StreamExecutionEnvironment.getExecutionEnvironment
  env.getConfig.setGlobalJobParameters(pl.param)
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  // env.enableCheckpointing(30000)
  // env.getCheckpointConfig.setMinPauseBetweenCheckpoints(15000)
  // env.getCheckpointConfig.setCheckpointTimeout(10000)
  // env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
  env.setStateBackend(new FsStateBackend(pl.stateBE, true))
  var jobs = List[(Int, String, String)]()
  def writeToOF(x : (DataStream[SAMRecord], String)) = {
    val wr = new it.crs4.jeenk.sam.CRAMWriter(pl.sref)
    val fname = pl.fout + x._2 + ".cram"
    val bucket = new BucketingSink[SAMRecord](fname)
      .setWriter(wr)
      .setBatchSize(1024 * 1024 * 8)
      .setPendingPrefix("")
      .setPendingSuffix("")
      .setBucketer(new BasePathBucketer)
      .setInactiveBucketCheckInterval(10000)
      .setInactiveBucketThreshold(10000)
    x._1
      .addSink(bucket)
      .setParallelism(pl.crampar)
      .name(fname)
  }
  def add(id : Int, topicname : String, filename : String) {
    jobs ::= (id, topicname, filename)
  }
  def doJob(id : Int, topicname : String, filename : String) = {
    val props = new ConsProps("outconsumer11.", pl.kafkaServer)
    props.put("auto.offset.reset", "earliest")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "10000")
    val cons = new FlinkKafkaConsumer011[SAMRecord](topicname, new MySDeserializer, props)
    val sam = env
      .addSource(cons)
      .setParallelism(pl.kafkaparW)
      .assignTimestampsAndWatermarks(new MyWaterMarker[SAMRecord])
      .name(topicname)

    writeToOF(sam, filename)
  }
  def go = {
    jobs.foreach(j => doJob(j._1, j._2, j._3))
    env.execute(s"CRAM Writer ${ind._1}/${ind._2}")
  }
}

class Writer(pl : Params) {
  def kafka2cram(toc : Array[String]) : Iterable[miniWriter] = {
    val filenames = toc
      .map(s => s.split(" ", 2))
      .map(r => (r.head, r.last)).toMap
    def RW(ids : Iterable[Int], ind : (Int, Int)) : miniWriter = {
      val mw = new miniWriter(pl, ind)
      ids.foreach{
        id =>
        val topicname = pl.kafkaAligned + "-" + id.toString
        mw.add(id, topicname, filenames(id.toString))
      }
      mw
    }
    val ids = filenames.keys.map(_.toInt)
    val g = ids.grouped(pl.wgrouping).toArray
    val n = g.size
    g.indices.map(i => RW(g(i), (i+1, n)))
  }
}

object runWriter {
  def main(args: Array[String]) {
    val pargs = ParameterTool.fromArgs(args)
    val propertiesFile = pargs.getRequired("properties")
    val pfile = ParameterTool.fromPropertiesFile(propertiesFile)
    val pl = new Params(pfile.mergeWith(pargs))

    implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(pl.numWriters))
    // implicit val timeout = Timeout(30 seconds)

    val rg = new scala.util.Random
    val cp = new ConsProps("conconsumer11.", pl.kafkaServer)
    cp.put("auto.offset.reset", "earliest")
    cp.put("enable.auto.commit", "true")
    cp.put("auto.commit.interval.ms", "10000")
    val conConsumer = new KafkaConsumer[Int, String](cp)
    val rw = new Writer(pl)
    conConsumer.subscribe(List(pl.kafkaControlAL))
    var jobs = List[Future[Any]]()
    val startTime = java.time.Instant.now.getEpochSecond
    var goOn = true
    while (goOn) {
      val records = conConsumer.poll(3000)
      jobs ++= records
        .filter(_.key == 1)
        .flatMap
      {
	r =>
	  rw.kafka2cram(r.value.split("\n"))
	    .map(j => Future{j.go})
      }
      // stay in loop until jobs are done or timeout not expired
      val now = java.time.Instant.now.getEpochSecond
      goOn = pl.cramwriterTimeout <= 0 || now < startTime + pl.cramwriterTimeout
      val aggregated = Future.sequence(jobs)
      try {
        Await.ready(aggregated, 100 millisecond)
      } catch {
        case e: TimeoutException => goOn = true
      }
    }
    conConsumer.close
  }
}

