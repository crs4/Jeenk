package bclconverter.kafka

import com.typesafe.config.ConfigFactory
import htsjdk.samtools.SAMRecord
import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.Executors
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.extensions._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner
import org.apache.flink.streaming.util.serialization.{KeyedSerializationSchema, KeyedDeserializationSchema, DeserializationSchema}
import org.apache.flink.util.MathUtils
import org.apache.hadoop.conf.{Configuration => HConf}
import org.apache.hadoop.fs.{FileSystem, Path => HPath}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.slf4j.LoggerFactory
import scala.collection.mutable.Map
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Await, Future}
import scala.io.Source
import scala.xml.{XML, Node}

import bclconverter.conf.Params.{Block, PRQData}

class MyKSerializer extends KeyedSerializationSchema[PRQData] {
  def toBytes(i : Int) : Array[Byte] = {
    ByteBuffer.allocate(4).putInt(i).array
  }
  override def serializeValue(x : PRQData) : Array[Byte] = {
    val r = toBytes(x._1.size) ++ x._1 ++
      toBytes(x._2.size) ++ x._2 ++
      toBytes(x._3.size) ++ x._3 ++
      toBytes(x._4.size) ++ x._4 ++
      toBytes(x._5.size) ++ x._5
    return r
  }
  override def serializeKey(x : PRQData) : Array[Byte] = null
  override def getTargetTopic(x : PRQData) : String = null
}

class MySSerializer extends KeyedSerializationSchema[SAMRecord] {
  override def serializeValue(x : SAMRecord) : Array[Byte] = {
    val bos = new java.io.ByteArrayOutputStream // buffer
    val out = new java.io.ObjectOutputStream(bos)
    out.writeObject(x)
    out.flush
    val r = bos.toByteArray
    return r
  }
  override def serializeKey(x : SAMRecord) : Array[Byte] = null
  override def getTargetTopic(x : SAMRecord) : String = null
}

class MyPartitioner[T](max : Int) extends FlinkKafkaPartitioner[T] {
  var p = 0
  override
  def partition(record : T, serKey : Array[Byte], serVal : Array[Byte], tarTopic : String, parts : Array[Int]) : Int = {
    val rp = p
    p = (p + 1) % max
    return rp
  }
}

object ProdProps {
  val rg = new scala.util.Random
}

class ProdProps(pref : String, kafkaServer : String) extends Properties {
  private val pkeys = Seq("bootstrap.servers", "acks", "compression.type", "key.serializer", "value.serializer",
    "batch.size", "linger.ms", "request.timeout.ms").map(pref + _)

  lazy val typesafeConfig = ConfigFactory.load()

  pkeys.map{ key =>
    if (typesafeConfig.hasPath(key))
      put(key.replace(pref, ""), typesafeConfig.getString(key))
  }
  put("client.id", "robo" + ProdProps.rg.nextLong.toString)
  put("retries", "5")
  put("max.in.flight.requests.per.connection", "1")
  put("bootstrap.servers", kafkaServer)


  def getCustomString(key: String) = typesafeConfig.getString(key)
  def getCustomInt(key: String) = typesafeConfig.getInt(key)
}

object MyDeserializer extends Serializable {
  def invertMurmur(ks : Int) : Array[Int] = {
    val inv = Map[Int, Int]()
    val max = org.apache.flink.runtime.state.KeyGroupRangeAssignment.computeDefaultMaxParallelism(ks)
    var i = 0
    while(inv.size != ks) {
      val h = (MathUtils.murmurHash(i) % max) * ks / max
      if (h <= ks)
        inv.getOrElseUpdate(h, i)
      i += 1
    }
    Range(0, ks).map(k => inv(k)).toArray
  }
  def init(ks : Int) {
    if (invMurmur == null)
      invMurmur = MyDeserializer.invertMurmur(ks)
  }
  val logger = LoggerFactory.getLogger("rootLogger")
  @volatile var invMurmur : Array[Int] = null
}

class MyDeserializer(val fpar : Int) extends DeserializationSchema[(Int, PRQData)] {
  var eos = false
  var key = 0
  var invMurmur : Array[Int] = null
  override def getProducedType = TypeInformation.of(classOf[(Int, PRQData)])
  override def isEndOfStream(el : (Int, PRQData)) : Boolean = {
    if (el._2._1.size > 1)
      false
    else
      true
  }
  override def deserialize(data : Array[Byte]) : (Int, PRQData) = {
    if (invMurmur == null)
      setMurmur
    val rkey = invMurmur(key)
    key = (key + 1) % fpar
    val (s1, r1) = data.splitAt(4)
    val (p1, d1) = r1.splitAt(toInt(s1))
    val (s2, r2) = d1.splitAt(4)
    val (p2, d2) = r2.splitAt(toInt(s2))
    val (s3, r3) = d2.splitAt(4)
    val (p3, d3) = r3.splitAt(toInt(s3))
    val (s4, r4) = d3.splitAt(4)
    val (p4, d4) = r4.splitAt(toInt(s4))
    val (s5, p5) = d4.splitAt(4)
    (rkey, (p1, p2, p3, p4, p5))
  }
  def toInt(a : Array[Byte]) : Int = {
    ByteBuffer.wrap(a).getInt
  }
  def setMurmur {
    MyDeserializer.init(fpar)
    invMurmur = MyDeserializer.invMurmur
  }
}
object MySDeserializer {
  val eos_tag = "EOS"
}

class MySDeserializer extends DeserializationSchema[SAMRecord] {
  override def getProducedType = TypeInformation.of(classOf[SAMRecord])
  override def isEndOfStream(el : SAMRecord) : Boolean = {
    if (el.getReadName != MySDeserializer.eos_tag)
      false
    else
      true
  }
  override def deserialize(data : Array[Byte]) : SAMRecord = {
    val bis = new java.io.ByteArrayInputStream(data)
    val in = new java.io.ObjectInputStream(bis)
    val r = in.readObject.asInstanceOf[SAMRecord]
    in.close
    return r
  }
}


object ConsProps {
  val rg = new scala.util.Random
}

class ConsProps(pref: String, kafkaServer : String) extends Properties {
  private val pkeys = Seq("bootstrap.servers", "group.id", "request.timeout.ms",
    "value.deserializer", "key.deserializer", "auto.offset.reset").map(pref + _)
  lazy val typesafeConfig = ConfigFactory.load()

  pkeys.map{ key =>
    if (typesafeConfig.hasPath(key)) {
      put(key.replace(pref, ""), typesafeConfig.getString(key))
    }
  }
  put("client.id", "robo" + ConsProps.rg.nextLong.toString)
  put("group.id", "robo" + ConsProps.rg.nextLong.toString)
  put("bootstrap.servers", kafkaServer)

  def getCustomString(key: String) = typesafeConfig.getString(key)
  def getCustomInt(key: String) = typesafeConfig.getInt(key)
}
