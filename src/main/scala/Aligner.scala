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

package it.crs4.jeenk.aligner

import com.typesafe.config.ConfigFactory
import htsjdk.samtools.SAMRecord
import java.util.concurrent.{Executors, TimeoutException}
import java.util.Properties
import org.apache.flink.api.java.utils.ParameterTool
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
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue
import org.apache.kafka.clients.admin._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Await, Future}
import scala.language.postfixOps

import it.crs4.jeenk.conf.Params
import it.crs4.jeenk.conf.Params.{Block, PRQData}
import it.crs4.jeenk.kafka.{MySSerializer, MyPartitioner, ProdProps, ConsProps, MyDeserializer, MySDeserializer}

class MyWaterMarker[T] extends AssignerWithPeriodicWatermarks[T] {
  var cur = 0l
  override def extractTimestamp(el: T, prev: Long): Long = {
    val r = cur
    cur += 1
    return r
  }
  override def getCurrentWatermark : Watermark = {
    new Watermark(cur - 1)
  }
}

class miniAligner(pl : Params, ind : (Int, Int)) {
  // initialize stream environment
  var env = StreamExecutionEnvironment.getExecutionEnvironment
  env.getConfig.setGlobalJobParameters(pl.param)
  env.setParallelism(pl.aflinkpar)
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  // env.enableCheckpointing(30000)
  // env.getCheckpointConfig.setMinPauseBetweenCheckpoints(15000)
  // env.getCheckpointConfig.setCheckpointTimeout(10000)
  // env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
  env.setStateBackend(new FsStateBackend(pl.stateBE, true))
  var jobs = List[(Int, String, String)]()
  def createKTopic(topic : String) = {
    val props = new Properties
    props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, pl.kafkaServer)
    val adminClient = AdminClient.create(props)
    val newTopic = new NewTopic(topic, pl.kafkaparout, 1)
    try {
      val fut = adminClient.createTopics(List(newTopic))
      fut.all.get
    } catch {
      case _ : java.util.concurrent.ExecutionException => // topic already exists
    }
  }
  def sendAligned(x : (DataStream[SAMRecord], Int)) = {
    val name = pl.kafkaAligned + "-" + x._2.toString
    createKTopic(name)
    val part : java.util.Optional[FlinkKafkaPartitioner[SAMRecord]] = java.util.Optional.of(new MyPartitioner[SAMRecord](pl.kafkaparout))
    val kprod = new FlinkKafkaProducer011(
      name,
      new MySSerializer,
      new ProdProps("outproducer11.", pl.kafkaServer),
      part
    )
    x._1.addSink(kprod)
      .name(name)
  }
  def add(id : Int, topicname : String, filename : String) {
    jobs ::= (id, topicname, filename)
  }
  def doJob(id : Int, topicname : String, filename : String) = {
    val props = new ConsProps("outconsumer11.", pl.kafkaServer)
    props.put("auto.offset.reset", "earliest")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "10000")
    val cons = new FlinkKafkaConsumer011[(Int, PRQData)](topicname, new MyDeserializer(pl.aflinkpar), props)
    val ds = env
      .addSource(cons)
      .setParallelism(pl.kafkaparA)
      .assignTimestampsAndWatermarks(new MyWaterMarker[(Int, PRQData)])
      .name(topicname)
    val sam = ds
      .keyBy(_._1)
      .timeWindow(Time.milliseconds(pl.rapiwin * pl.aflinkpar / pl.kafkaparA))
      .apply(new it.crs4.jeenk.sam.PRQAligner[TimeWindow, Int](pl.sref, pl.rapipar))

    // send data to topic
    sendAligned(sam, id)
  }
  // send EOS to each kafka partition, for each topic
  def sendEOS(id : Int) = {
    env.setParallelism(1)
    val name = pl.kafkaAligned + "-" + id.toString
    val eos  = new SAMRecord(null)
    eos.setReadName(MySDeserializer.eos_tag)
    val EOS : DataStream[SAMRecord] = env.fromCollection(Array.fill(pl.kafkaparout)(eos))
    EOS.name("EOS")
    val part : java.util.Optional[FlinkKafkaPartitioner[SAMRecord]] = java.util.Optional.of(new MyPartitioner[SAMRecord](pl.kafkaparout))
    val kprod = new FlinkKafkaProducer011(
      name,
      new MySSerializer,
      new ProdProps("outproducer11.", pl.kafkaServer),
      part
    )
    EOS.addSink(kprod)
       .name(name)
  }
  def go = {
    jobs.foreach(j => doJob(j._1, j._2, j._3))
    env.execute(s"Aligner ${ind._1}/${ind._2}")
    jobs.foreach(j => sendEOS(j._1))
    env.execute(s"Send EOS ${ind._1}/${ind._2}")
  }
}

class Aligner(pl : Params) {
  val conProducer = new KafkaProducer[Int, String](new ProdProps("conproducer11.", pl.kafkaServer))
  def kafkaAligner(toc : Array[String]) : Iterable[miniAligner] = {
    val filenames = toc
      .map(s => s.split(" ", 2))
      .map(r => (r.head, r.last)).toMap
    def RW(ids : Iterable[Int], ind : (Int, Int)) : miniAligner = {
      val mw = new miniAligner(pl, ind)
      ids.foreach{
        id =>
        val topicname = pl.kafkaTopic + "-" + id.toString
        mw.add(id, topicname, filenames(id.toString))
      }
      mw
    }
    conProducer.send(new ProducerRecord(pl.kafkaControlAL, 1, toc.mkString("\n")))
    val ids = filenames.keys.map(_.toInt)
    val g = ids.grouped(pl.agrouping).toArray
    val n = g.size
    g.indices.map(i => RW(g(i), (i+1, n)))
  }
}

object runAligner {
  def main(args: Array[String]) {
    val pargs = ParameterTool.fromArgs(args)
    val propertiesFile = pargs.getRequired("properties")
    val pfile = ParameterTool.fromPropertiesFile(propertiesFile)
    val pl = new Params(pfile.mergeWith(pargs))

    implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(pl.numAligners))
    // implicit val timeout = Timeout(30 seconds)

    val rg = new scala.util.Random
    val cp = new ConsProps("conconsumer11.", pl.kafkaServer)
    cp.put("auto.offset.reset", "earliest")
    cp.put("enable.auto.commit", "true")
    cp.put("auto.commit.interval.ms", "10000")
    val conConsumer = new KafkaConsumer[Int, String](cp)
    val rw = new Aligner(pl)
    conConsumer.subscribe(List(pl.kafkaControlPRQ))
    var jobs = List[Future[Any]]()
    val startTime = java.time.Instant.now.getEpochSecond
    var goOn = true

    while (goOn) {
      val records = conConsumer.poll(3000)
      jobs ++= records
        .filter(_.key == 0)
        .flatMap
      {
	r =>
	  rw.kafkaAligner(r.value.split("\n"))
	    .map(j => Future{j.go})
      }
      // stay in loop until jobs are done or timeout not expired
      val now = java.time.Instant.now.getEpochSecond
      goOn = pl.alignerTimeout <= 0 || now < startTime + pl.alignerTimeout
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

