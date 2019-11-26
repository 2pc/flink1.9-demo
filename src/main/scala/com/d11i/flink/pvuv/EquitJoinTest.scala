/*
 * Created by IntelliJ IDEA.
 * User: execlu
 * Date: 2019/11/15
 * Time: 17:34
 */
package com.d11i.flink.pvuv

import java.sql.Timestamp

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

import scala.collection.mutable

object EquitJoinTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    val ordersData = new mutable.MutableList[(String, String, Long)]
    ordersData.+=(("001", "iphone", 1545800002000L))
    ordersData.+=(("002", "mac", 1545800003000L))
    ordersData.+=(("003", "book", 1545800004000L))
    ordersData.+=(("004", "cup", 1545800018000L))

    // 构造付款表
    val paymentData = new mutable.MutableList[(String, String, Long)]
    paymentData.+=(("001", "alipay", 1545803501000L))
    paymentData.+=(("002", "card", 1545803602000L))
    paymentData.+=(("003", "card", 1545803610000L))
    paymentData.+=(("004", "alipay", 1545803611000L))
    val orders = env
      .fromCollection(ordersData)
      .toTable(tEnv, 'orderId, 'productName, 'orderTime)
    val ratesHistory = env
      .fromCollection(paymentData)
      .toTable(tEnv, 'orderId, 'payType, 'payTime)

    tEnv.registerTable("Orders", orders)
    tEnv.registerTable("Payment", ratesHistory)
    var sqlQuery3 =
      """
        |SELECT
        | *
        |FROM
        | Orders AS o left   JOIN Payment AS p ON
        | o.orderId = p.orderId
        |""".stripMargin

    val sqlRet2 = tEnv.sqlQuery(sqlQuery3)
    //println(tEnv.explain(sqlRet2))
    println(sqlRet2.toRetractStream[Row])
  }

  case class Order(user: Long, product: String, amount: Int)
}

