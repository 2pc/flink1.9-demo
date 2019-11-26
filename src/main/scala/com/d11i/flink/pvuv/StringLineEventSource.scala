package com.d11i.flink.pvuv

import java.time.Instant
import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
//import org.slf4j.LoggerFactory

import scala.util.Random

class StringLineEventSource(val latenessMillis: Long) extends RichParallelSourceFunction[String] {

//  val LOG = LoggerFactory.getLogger(classOf[StringLineEventSource])
  @volatile private var running = true
  val channelSet = Seq("a", "b", "c", "d")
  val behaviorTypes = Seq(
    "INSTALL", "OPEN", "BROWSE", "CLICK",
    "PURCHASE", "CLOSE", "UNINSTALL")
  val rand = Random

  override def run(ctx: SourceContext[String]): Unit = {
    val numElements = Long.MaxValue
    var count = 0L

    while (running && count < numElements) {
      val channel = channelSet(rand.nextInt(channelSet.size))
      val event = generateEvent()
//      LOG.debug("Event: " + event)
      val ts = event(0)
      val id = event(1)
      val behaviorType = event(2)
      ctx.collect(Seq(ts, channel, id, behaviorType).mkString("\t"))
      count += 1
      TimeUnit.MILLISECONDS.sleep(5L)
    }
  }

  private def generateEvent(): Seq[String] = {
    // simulate 10 seconds lateness
    val ts = Instant.ofEpochMilli(System.currentTimeMillis)
      .minusMillis(latenessMillis)
      .toEpochMilli

    val id = UUID.randomUUID().toString
    val behaviorType = behaviorTypes(rand.nextInt(behaviorTypes.size))
    // (ts, id, behaviorType)
    Seq(ts.toString, id, behaviorType)
  }

  override def cancel(): Unit = running = false
}
