package com.d11i.flink.pvuv

import java.text.SimpleDateFormat

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.function.RichWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


class MyAccumulatingWindowFunction extends RichWindowFunction[(String, Long),(String,String,String,Int, String, Int),String,TimeWindow]{

  val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

  var state: ValueState[Int] = _

  var count = 0

  override def open(config: Configuration): Unit = {
    state = getRuntimeContext.getState(new ValueStateDescriptor[Int]("AccumulatingWindow Test", classOf[Int], 0))
  }

  override def apply(key: String, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[(String, String, String, Int, String, Int)]): Unit = {
    count = state.value() + input.size

    state.update(count)

    // key,window start time, window end time, window size, system time, total size
    out.collect(key, format.format(window.getStart),format.format(window.getEnd),input.size, format.format(System.currentTimeMillis()),count)
  }
}