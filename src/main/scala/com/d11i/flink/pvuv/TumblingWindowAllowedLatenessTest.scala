package com.d11i.flink.pvuv

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.mutable

/**
  * windowedStream.allowedLateness() test,this is also called window accumulating.
  * allowedLateness will trigger window again when 'late element' arrived to the window
  */
object TumblingWindowAllowedLatenessTest {

  def  main(args : Array[String]) : Unit = {

//    if (args.length != 2) {
//      System.err.println("USAGE:\nSocketTextStreamWordCount <hostname> <port>")
//      return
//    }
//
//    val hostName = args(0)
//    val port = args(1).toInt


    val env = StreamExecutionEnvironment.getExecutionEnvironment //获取流处理执行环境
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) //设置Event Time作为时间属性
    //env.setBufferTimeout(10)
    //env.enableCheckpointing(5000,CheckpointingMode.EXACTLY_ONCE)

//    val input = env.socketTextStream(hostName,port) //socket接收数据

    val data = new mutable.MutableList[(String, Long)]
    data.+=(("A", 1570365660000L)) // no joining record
    data.+=(("A", 1570365663000L)) // no joining record
    data.+=(("A", 1570365672000L)) // no joining record
    data.+=(("A", 1570365675000L)) // no joining record
    data.+=(("A", 1570365666000L)) // no joining record
    data.+=(("A", 1570365669000L)) // no joining record
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

    println("data :")
    data.map(x =>{
      //val  date:Date =  new Date(x._3)
      println((x._1,format.format(x._2)))
    })

//    val inputMap = input.map(f=> {
//      val arr = f.split("\\W+")
//      val code = arr(0)
//      val time = arr(1).toLong
//      (code,time)
//    })

    /**
      * 允许3秒的乱序
      */
    val watermarkDS = env.fromCollection(data).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String,Long)] {

      var currentMaxTimestamp = 0L
      val maxOutOfOrderness = 3000L

      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

      override def getCurrentWatermark: Watermark = {
        new Watermark(currentMaxTimestamp - maxOutOfOrderness)
      }

      override def extractTimestamp(t: (String,Long), l: Long): Long = {
        val timestamp = t._2
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
        timestamp
      }
    })

    /**
      * 对于此窗口而言，允许5秒的迟到数据，即第一次触发是在watermark > end-of-window时
      * 第二次（或多次）触发的条件是watermark < end-of-window + allowedLateness时间内，这个窗口有late数据到达
      */
    val accumulatorWindow = watermarkDS
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .allowedLateness(Time.seconds(5))
      .apply(new MyAccumulatingWindowFunction)
      .name("window accumulate test")
      .setParallelism(2)

    accumulatorWindow.print()


    env.execute()

  }
}
