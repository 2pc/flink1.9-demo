package com.d11i.flink.pvuv

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.scala._

import scala.collection.mutable

object TumbleTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
   // env.setStateBackend(getStateBackend)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
   // StreamITCase.clear
    val data1 = new mutable.MutableList[(String, String, Long)]
    data1.+=(("A", "L-7", 1570365660001L))
    data1.+=(("B", "L-7", 1570365665000L))
    data1.+=(("C", "L-7", 1572336777999L)) // no joining record

    data1.+=(("D", "L-7", 1572337111001L))
//    data1.+=(("D", "L-7", 1510365661000L))
//    data1.+=(("D", "L-7", 1510365665000L))

    val data2 = new mutable.MutableList[(String, String, Long)]
//    data2.+=(("A", "R-1", 7000L)) // 3 joining records
//    data2.+=(("B", "R-4", 7000L)) // 1 joining records
//    data2.+=(("A", "R-3", 8000L)) // 3 joining records
//    data2.+=(("D", "R-2", 8000L)) // no joining record
    data2.+=(("A", "R-2", 1570365660000L)) // no joining record
    data2.+=(("A", "R-2", 1570365663000L)) // no joining record
    data2.+=(("A", "R-2", 1570365664000L)) // no joining record
    data2.+=(("B", "R-2", 1570365665000L)) // no joining record1572336778
    data2.+=(("C", "R-2", 1572336778000L)) // no joining record
    data2.+=(("D", "R-2", 1572337111000L))
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

    println("data1:")
    data1.map(x =>{
      val  date:Date =  new Date(x._3)
      println((x._1,x._2,format.format(date)))
    })
    println("data2:")
    data2.map(x =>{
      val  date:Date =  new Date(x._3)
      println((x._1,x._2,format.format(date)))
    })
    val t1 = env.fromCollection(data1)
      .assignTimestampsAndWatermarks(new Row3WatermarkExtractor2)
      .toTable(tEnv, 'key, 'id, 'rt.rowtime)
    val t2 = env.fromCollection(data2)
      .assignTimestampsAndWatermarks(new Row3WatermarkExtractor2)
      .toTable(tEnv, 'key, 'id, 'rt.rowtime)

    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)
    val sqlQuery =
      """
        |SELECT t2.key,TUMBLE_START(t2.rt, INTERVAL '4' SECOND),TUMBLE_END(t2.rt, INTERVAL '4' SECOND), COUNT(t1.key)
        |FROM T1 AS t1 join T2 AS t2 ON
        |  t1.key = t2.key AND
        |  t1.rt BETWEEN t2.rt - INTERVAL '3' SECOND AND
        |    t2.rt + INTERVAL '3' SECOND
        |GROUP BY TUMBLE(t2.rt, INTERVAL '4' SECOND),t2.key
        |""".stripMargin

    val result = tEnv.sqlQuery(sqlQuery)

   // println(tEnv.explain(result))
    val ret=result.toAppendStream[Row]
    System.out.println(tEnv.explain(result))
    ret.print()
    env.execute("")
  }

}
private class Row3WatermarkExtractor2
  extends AssignerWithPunctuatedWatermarks[(String, String, Long)] {

  override def checkAndGetNextWatermark(
                                         lastElement: (String, String, Long),
                                         extractedTimestamp: Long): Watermark = {
    new Watermark(extractedTimestamp - 1)
  }

  override def extractTimestamp(
                                 element: (String, String, Long),
                                 previousElementTimestamp: Long): Long = {
    element._3
  }
}