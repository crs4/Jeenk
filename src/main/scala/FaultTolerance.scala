package bclconverter.aligner

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.checkpoint.{ListCheckpointed, CheckpointedFunction}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{WindowFunction, AllWindowFunction}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{Window, TimeWindow, GlobalWindow}
import org.apache.flink.streaming.connectors.fs.bucketing.{BucketingSink, DateTimeBucketer}
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue
import org.apache.flink.util.Collector
import org.apache.hadoop.conf.{Configuration => HConf}
import scala.collection.JavaConversions._   
       

class MyWM extends AssignerWithPeriodicWatermarks[(Int, String)] {
  var cur = 0l
  override def extractTimestamp(el: (Int, String), prev: Long): Long = {
    val r = cur
    cur += 1
    return r
  }
  override def getCurrentWatermark : Watermark = {
    new Watermark(cur - 1)
  }
}

class MyWM2 extends AssignerWithPunctuatedWatermarks[(Int, String)] {
  var cur = 0l
  override def extractTimestamp(el: (Int, String), prev: Long): Long = {
    val r = cur
    cur += 1
    return r
  }
  override def checkAndGetNextWatermark(el: (Int, String), prev: Long) : Watermark = {
    if (el._2 == "EOS") {
      println("############### EOS")
      Watermark.MAX_WATERMARK
    }
    else
      null
    // new Watermark(cur - 1)
  }
}

class winTest[W <: Window] extends WindowFunction[(Int, String), String, Tuple, W] {
  def apply(key : Tuple, w : W, in : Iterable[(Int, String)], out : Collector[String]) = {
    in.foreach(x => out.collect(x._2))
  }
}

object MySource {
  val rg = new scala.util.Random
}

// checkpointed source
class MyCPSource(len : Long) extends SourceFunction[(Int, String)] with ListCheckpointed[java.lang.Long] {
  import MySource.rg
  private var isRunning = true
  private var cow :java.lang.Long = 0l

  override def run(ctx: SourceFunction.SourceContext[(Int, String)]) {
    val lock = ctx.getCheckpointLock
    while (isRunning && cow < len) {
      lock.synchronized {
        // ctx.collect((rg.nextInt(16), rg.nextLong.toString))
        ctx.collect((rg.nextInt(16), s"$cow: ${rg.nextInt(10)}"))
        cow += 1
        Thread.sleep(10)
      }
    }
    // ctx.collect((0, "EOS"))
    Thread.sleep(10000)
  }
  def cancel() {
    isRunning = false
  }
  override def restoreState(state: java.util.List[java.lang.Long]) {
    for (s <- state) {
      cow = s
    }
  }
  override def snapshotState(checkpointId: Long, timestamp: Long): java.util.List[java.lang.Long] = {
    java.util.Collections.singletonList(cow)
  }
}


object faultTest {
  def main(args: Array[String]) {
    // command line parameters
    val param = ParameterTool.fromArgs(args)
    val size = param.getLong("len", 10000)
    val msec = 1000 * param.getLong("chk", 5)
    val sbe = param.get("sbe", "file:///tmp/flink-state-backend")
    val sname = param.get("sname", "file:///tmp/flink-out.txt")

    // setup flink environment
    var env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(msec)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(msec/2)
    env.getCheckpointConfig.setCheckpointTimeout(msec/2)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.setStateBackend(new FsStateBackend(sbe, true))

    // generate data and write to disk
    val ds = env.addSource(new MyCPSource(size))
      .assignTimestampsAndWatermarks(new MyWM)
    val out = ds
      .keyBy(0)
      .timeWindow(Time.milliseconds(1024))
      .apply(new winTest[TimeWindow])
    val bucket = new BucketingSink[String](sname)
      .setBucketer(new DateTimeBucketer("yyyy"))
      .setBatchSize(1024)
      .setInactiveBucketCheckInterval(1000)
      .setInactiveBucketThreshold(1000)
    out
      .addSink(bucket)
      .name("Writing output")

    env.execute(s"bucket test")
  }
}
