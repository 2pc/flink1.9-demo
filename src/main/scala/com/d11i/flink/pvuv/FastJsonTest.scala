package com.d11i.flink.pvuv

import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject}

object FastJsonTest {

  def main(args: Array[String]): Unit = {
    val body:JSONObject= new JSONObject()
    val header :JSONObject= new JSONObject()
    val msg :JSONObject= new JSONObject()
    body.put("aa","aaa")
    body.put("aanull",null)
    header.put("aa","aaa")
    header.put("aanull",null)
    msg.put("body",body)
    msg.put("header",header)
    //println(msg.toJSONString)
    //println(JSON.toJSONString(msg))
    println(JSON.toJSONString(msg, SerializerFeature.WriteMapNullValue) )
  }

}