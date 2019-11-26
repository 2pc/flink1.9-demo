package com.d11i.flink.pvuv

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.scala._

/**
  * Simple example for demonstrating the use of Table API on a Stream Table.
  *
  * This example shows how to:
  *  - Convert DataStreams to Tables
  *  - Apply union, select, and filter operations
  */
object NoWindowJoin {

  // *************************************************************************
  //     PROGRAM
  // *************************************************************************

  def main(args: Array[String]): Unit = {

    // set up execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)

    val orderA = env.fromCollection(Seq(
      OrderA(1L, "beer", 3),
      OrderA(1L, "diaper", 4),
      OrderA(3L, "rubber", 2)))
      //.toTable(tEnv,'aId,'product1,'amount1)

    val orderB = env.fromCollection(Seq(
      OrderB(1L, "pen", 3),
      OrderB(2L, "rubber", 3),
      OrderB(4L, "beer", 1)))
      //.toTable(tEnv,'bId,'product2,'amount2)

    tEnv.registerDataStream("orderA",orderA)
    tEnv.registerDataStream("orderB",orderB)
    val result = tEnv.sqlQuery("select orderA.aId as aId,orderB.bId as bId, product1, amount1 from orderA  right join orderB on orderA.aId = orderB.bId" )
//    tEnv.registerDataStream("retTable",result.toRetractStream[OrderRet]);
//    // union the two tables
//    val result: DataStream[OrderRet] = orderA.join(orderB).where('aId === 'bId)
//      .select( 'aId,'bId,'product1, 'amount1)
//     // .where('amount > 2)
//      .toAppendStream[OrderRet]
//
//    tEnv.sqlQuery("select aId,bId,product1,amount1 from retTable where amount1>3")
    //println(tEnv.explain(result))
    result.toRetractStream[OrderRet].print()

    env.execute()
  }

  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************

  case class OrderA(aId: Long, product1: String, amount1: Int)

  case class OrderB(bId: Long, product2: String, amount2: Int)

  case class OrderRet(aId: Long,bId: Long, product1: String, amount1: Int)

}

