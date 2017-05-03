package bclconverter.fp

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.ExecutionMode._

class Fenv {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
}
