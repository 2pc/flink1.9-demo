package com.d11i.flink.pvuv

import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.types.Row
//import org.apache.flink.table.api.TableEnvironment

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._

import scala.collection.mutable

object FlinkSQLTest {

  def main(args: Array[String]): Unit = {
//      procTimePrint("SELECT c_name, CONCAT(c_name, ' come ', c_desc) as desc  FROM customer_tab")
//    println("SELECT c_id, count(o_id) as o_count FROM order_tab GROUP BY c_id")
//     procTimePrint("SELECT c_id, count(o_id) as o_count FROM order_tab GROUP BY c_id")
    println("SELECT * FROM customer_tab AS c JOIN order_tab AS o ON o.c_id = c.c_id")
    procTimePrint("SELECT * FROM customer_tab AS c JOIN order_tab AS o ON o.c_id = c.c_id")
    // OVER windows can only be applied on time attributes.
   // rowTimePrint("SELECT  \n    itemID,\n    itemType, \n    onSellTime, \n    price,  \n    MAX(price) OVER (\n        PARTITION BY itemType \n        ORDER BY onSellTime \n        ROWS BETWEEN 2 preceding AND CURRENT ROW) AS maxPrice\n  FROM item_tab")
    rowTimePrint("SELECT  \n    region,\n    TUMBLE_START(rowtime, INTERVAL '2' MINUTE) AS winStart,  \n    TUMBLE_END(rowtime, INTERVAL '2' MINUTE) AS winEnd,  \n    COUNT(region) AS pv\nFROM pageAccess_tab" +
      " \nGROUP BY region, TUMBLE(rowtime, INTERVAL '2' MINUTE)")

  }


  def getStateBackend: StateBackend = {
    new MemoryStateBackend()
  }

  // 客户表数据
  val customer_data = new mutable.MutableList[(String, String, String)]
  customer_data.+=(("c_001", "Kevin", "from JinLin"))
  customer_data.+=(("c_002", "Sunny", "from JinLin"))
  customer_data.+=(("c_003", "JinCheng", "from HeBei"))


  // 订单表数据
  val order_data = new mutable.MutableList[(String, String, String, String)]
  order_data.+=(("o_001", "c_002", "2018-11-05 10:01:01", "iphone"))
  order_data.+=(("o_002", "c_001", "2018-11-05 10:01:55", "ipad"))
  order_data.+=(("o_003", "c_001", "2018-11-05 10:03:44", "flink book"))

  // 商品销售表数据
  val item_data = Seq(
    Left((1510365660000L, (1510365660000L, 20, "ITEM001", "Electronic"))),
    Right((1510365660000L)),
    Left((1510365720000L, (1510365720000L, 50, "ITEM002", "Electronic"))),
    Right((1510365720000L)),
    Left((1510365780000L, (1510365780000L, 30, "ITEM003", "Electronic"))),
    Left((1510365780000L, (1510365780000L, 60, "ITEM004", "Electronic"))),
    Right((1510365780000L)),
    Left((1510365900000L, (1510365900000L, 40, "ITEM005", "Electronic"))),
    Right((1510365900000L)),
    Left((1510365960000L, (1510365960000L, 20, "ITEM006", "Electronic"))),
    Right((1510365960000L)),
    Left((1510366020000L, (1510366020000L, 70, "ITEM007", "Electronic"))),
    Right((1510366020000L)),
    Left((1510366080000L, (1510366080000L, 20, "ITEM008", "Clothes"))),
    Right((151036608000L)))

  // 页面访问表数据
  val pageAccess_data = Seq(
    Left((1510365660000L, (1510365660000L, "ShangHai", "U0010"))),
    Right((1510365660000L)),
    Left((1510365660000L, (1510365660000L, "BeiJing", "U1001"))),
    Right((1510365660000L)),
    Left((1510366200000L, (1510366200000L, "BeiJing", "U2032"))),
    Right((1510366200000L)),
    Left((1510366260000L, (1510366260000L, "BeiJing", "U1100"))),
    Right((1510366260000L)),
    Left((1510373400000L, (1510373400000L, "ShangHai", "U0011"))),
    Right((1510373400000L)))

  // 页面访问量表数据2
  val pageAccessCount_data = Seq(
    Left((1510365660000L, (1510365660000L, "ShangHai", 100))),
    Right((1510365660000L)),
    Left((1510365660000L, (1510365660000L, "BeiJing", 86))),
    Right((1510365660000L)),
    Left((1510365960000L, (1510365960000L, "BeiJing", 210))),
    Right((1510366200000L)),
    Left((1510366200000L, (1510366200000L, "BeiJing", 33))),
    Right((1510366200000L)),
    Left((1510373400000L, (1510373400000L, "ShangHai", 129))),
    Right((1510373400000L)))

  // 页面访问表数据3
  val pageAccessSession_data = Seq(
    Left((1510365660000L, (1510365660000L, "ShangHai", "U0011"))),
    Right((1510365660000L)),
    Left((1510365720000L, (1510365720000L, "ShangHai", "U0012"))),
    Right((1510365720000L)),
    Left((1510365720000L, (1510365720000L, "ShangHai", "U0013"))),
    Right((1510365720000L)),
    Left((1510365900000L, (1510365900000L, "ShangHai", "U0015"))),
    Right((1510365900000L)),
    Left((1510366200000L, (1510366200000L, "ShangHai", "U0011"))),
    Right((1510366200000L)),
    Left((1510366200000L, (1510366200000L, "BeiJing", "U2010"))),
    Right((1510366200000L)),
    Left((1510366260000L, (1510366260000L, "ShangHai", "U0011"))),
    Right((1510366260000L)),
    Left((1510373760000L, (1510373760000L, "ShangHai", "U0410"))),
    Right((1510373760000L)))

  def procTimePrint(sql: String): Unit = {
    // Streaming 环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    //val tEnv = TableEnvironment.getTableEnvironment(env)

    // 将order_tab, customer_tab 注册到catalog
//    val customer = env.fromCollection(customer_data).toTable(tEnv).as('c_id, 'c_name, 'c_desc)
//    val order = env.fromCollection(order_data).toTable(tEnv).as('o_id, 'c_id, 'o_time, 'o_desc)

    val customer = env.fromCollection(customer_data).toTable(tEnv,'c_id, 'c_name, 'c_desc)
    val order = env.fromCollection(order_data).toTable(tEnv,'o_id, 'c_id, 'o_time, 'o_desc)

    tEnv.registerTable("order_tab", order)
    tEnv.registerTable("customer_tab", customer)

    val result = tEnv.sqlQuery(sql)
   // println(tEnv.explain(result))
    val ret = result.toRetractStream[Row]
    ret.print()
    env.execute()
  }

  def rowTimePrint(sql: String): Unit = {
    // Streaming 环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStateBackend(getStateBackend)
    env.setParallelism(1)
    val tEnv = StreamTableEnvironment.create(env)

    // 将item_tab, pageAccess_tab 注册到catalog
    val item =
      env.addSource(new EventTimeSourceFunction[(Long, Int, String, String)](item_data))
        .toTable(tEnv, 'onSellTime, 'price, 'itemID, 'itemType, 'rowtime.rowtime)

    val pageAccess =
      env.addSource(new EventTimeSourceFunction[(Long, String, String)](pageAccess_data))
        .toTable(tEnv, 'accessTime, 'region, 'userId, 'rowtime.rowtime)

    val pageAccessCount =
      env.addSource(new EventTimeSourceFunction[(Long, String, Int)](pageAccessCount_data))
        .toTable(tEnv, 'accessTime, 'region, 'accessCount, 'rowtime.rowtime)

    val pageAccessSession =
      env.addSource(new EventTimeSourceFunction[(Long, String, String)](pageAccessSession_data))
        .toTable(tEnv, 'accessTime, 'region, 'userId, 'rowtime.rowtime)

    tEnv.registerTable("item_tab", item)
    tEnv.registerTable("pageAccess_tab", pageAccess)
    tEnv.registerTable("pageAccessCount_tab", pageAccessCount)
    tEnv.registerTable("pageAccessSession_tab", pageAccessSession)

    val result = tEnv.sqlQuery(sql).toRetractStream[Row]
    result.print()
    //    val sink = new RetractingSink
    //    result.addSink(sink)
    env.execute()
  }

  }
