package com.d11i.flink.pvuv

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class EventTimeWindowReduceFunction extends ProcessWindowFunction[((String, String), Long), (Long,Long,String,String,Long), Tuple, TimeWindow] {

  override def process(key: Tuple, context: Context, elements: Iterable[((String, String), Long)], out: Collector[(Long,Long,String,String,Long)]): Unit = {
//    var count: Int = 0
//    for (in <- input) {
//
//      count = count + 1
//
//    }
//    out.collect(s"Window ${context.window} count: $count")
    key.getField(1)
    var count = 0L
    elements.foreach(x =>{
      count = count +1
    })
    val channel:String = key.getField(0)

    val behaviorType:String = key.getField(1)
    //val ret =new Tuple5 (context.window.getStart,context.window.getEnd, channel,behaviorType,count)
    out.collect(context.window.getStart,context.window.getEnd, channel,behaviorType,count)

  }

}
