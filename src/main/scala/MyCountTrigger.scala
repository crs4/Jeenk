package bclconverter

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.triggers.Trigger.{TriggerContext, OnMergeContext}
import org.apache.flink.annotation.PublicEvolving
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.{ReducingState, ReducingStateDescriptor}
import org.apache.flink.api.common.typeutils.base.LongSerializer
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.streaming.api.windowing.windows.Window
import scala.collection.JavaConversions._

import bclconverter.reader.Reader.{Block, PRQData}

class MyCountTrigger[W <: Window](maxCount : Long) extends Trigger[(Int, PRQData), W] {
  private val serialVersionUID = 1L

  val ls : TypeSerializer[java.lang.Long] = LongSerializer.INSTANCE
  private final val stateDesc : ReducingStateDescriptor[java.lang.Long]  =
    new ReducingStateDescriptor("count", new (MyCountTrigger.Sum), ls)

  override
  def onElement(element : (Int, PRQData), timestamp : Long, window : W, ctx: TriggerContext) : TriggerResult = {
    val count : ReducingState[java.lang.Long] = ctx.getPartitionedState(stateDesc)
    count.add(1L)
    val c = count.get
    val chk = element._2._1.size
    if (c >= maxCount || chk == 1) {
      count.clear()
      TriggerResult.FIRE_AND_PURGE
    }
    else
      TriggerResult.CONTINUE
  }

  override
  def onEventTime(time : Long, window : W, ctx : TriggerContext) : TriggerResult = {
    TriggerResult.CONTINUE
  }

  override
  def onProcessingTime(time : Long, window : W, ctx : TriggerContext) : TriggerResult = {
    TriggerResult.CONTINUE
  }

  override
  def clear(window : W, ctx : TriggerContext) = {
    ctx.getPartitionedState(stateDesc).clear()
  }

  override
  def canMerge : Boolean = {
    true
  }

  override
  def  onMerge(window : W, ctx : OnMergeContext) = {
    ctx.mergePartitionedState(stateDesc)
  }

  override
  def  toString : String = {
    "MyCountTrigger(" +  maxCount + ")"
  }

}

object MyCountTrigger {
  def of[W <: Window](maxCount : Long) : MyCountTrigger[W] = {
    new MyCountTrigger(maxCount)
  }

  private class Sum extends ReduceFunction[java.lang.Long] {
    private final val serialVersionUID = 1L

    override
    def reduce(value1 : java.lang.Long, value2 : java.lang.Long) : java.lang.Long = {
      value1 + value2
    }

  }
}
