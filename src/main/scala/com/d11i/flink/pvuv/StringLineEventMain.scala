package com.d11i.flink.pvuv

import java.text.SimpleDateFormat

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

object StringLineEventMain {

  var maxLaggedTimeMillis =0L
  val DEFAULT_MAX_LAGGED_TIME =5000;
  def main(args: Array[String]): Unit = {

    val params = ParameterTool.fromArgs(args)
    //checkParams(params)
    val sourceLatenessMillis = params.getRequired("source-lateness-millis").toLong
    maxLaggedTimeMillis = params.getLong("window-lagged-millis", DEFAULT_MAX_LAGGED_TIME)
    val windowSizeMillis = params.getRequired("window-size-millis").toLong

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) // 设置为TimeCharacteristic.EventTime

    val stream: DataStream[String] = env.addSource(new StringLineEventSource(sourceLatenessMillis))

    // create a Kafka producer for Kafka 0.9.x
    //    val kafkaProducer = new FlinkKafkaProducer09(
    //      params.getRequired("window-result-topic"),
    //      new SimpleStringSchema, params.getProperties
    //    )

    stream
      .setParallelism(1)
      .assignTimestampsAndWatermarks( // 指派时间戳，并生成WaterMark
        new BoundedOutOfOrdernessTimestampExtractor[String](Time.milliseconds(maxLaggedTimeMillis)) {
          override def extractTimestamp(element: String): Long = {
            element.split("\t")(0).toLong
          }
        })
      .setParallelism(2)
      .map(line => {
        // ts, channel, id, behaviorType
        val a = line.split("\t")
        val channel = a(1)
        ((channel, a(3)), 1L)
      })
      .setParallelism(3)
      .keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.milliseconds(windowSizeMillis))) // 使用Keyed Window
      .process(new EventTimeWindowReduceFunction).setParallelism(4)
      .map(t => {
        val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
        val windowStart = format.format(t._1)
        val windowEnd =  format.format(t._2)
        val channel = t._3
        val behaviorType = t._4
        val count = t._5



        Seq(windowStart, windowEnd, channel, behaviorType, count).mkString("\t")
      })
      .setParallelism(3)
      .print()
    //.setParallelism(3)

    env.execute(getClass.getSimpleName)
  }
}
