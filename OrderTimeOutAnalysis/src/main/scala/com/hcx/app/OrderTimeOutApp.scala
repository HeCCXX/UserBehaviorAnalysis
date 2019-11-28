package com.hcx.app

import com.hcx.dao.{OrderEvent, OrderResult}
import org.apache.flink.streaming.api.{TimeCharacteristic}
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.windowing.time.Time
object OrderTimeOutApp {

  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    environment.setParallelism(1)

    val dstream: DataStream[OrderEvent] = environment.fromCollection(List(
      OrderEvent("1", "create", 1558430842),
      OrderEvent("2", "create", 1558430843),
      OrderEvent("2", "pay", 1558430844)
    ))
    val keyStream: KeyedStream[OrderEvent, String] = dstream.assignAscendingTimestamps(_.timestamp * 1000).keyBy(_.userId)

    //定义匹配表达式
    val pattern: Pattern[OrderEvent, OrderEvent] = Pattern.begin[OrderEvent]("start").where(_.eventType == "create")
      .followedBy("follow").where(_.eventType == "pay")
      .within(Time.minutes(15))

    //定义输出标签，用于标明侧输出流
    val outputTag: OutputTag[OrderResult] = OutputTag[OrderResult]("orderTimeOut")

    //匹配数据流中的数据
    val pStream: PatternStream[OrderEvent] = CEP.pattern(keyStream,pattern)
    import scala.collection._
    val resultDstream: DataStream[OrderResult] = pStream.select(outputTag)(
      (pattern: Map[String, Iterable[OrderEvent]], timestamp: Long) => {
        val event: OrderEvent = pattern.getOrElse("start", null).iterator.next()
        OrderResult(event.userId, "timeout", timestamp)
      }
    )(
      (pattern: Map[String, Iterable[OrderEvent]]) => {
        val event: OrderEvent = pattern.getOrElse("follow", null).iterator.next()
        OrderResult(event.userId, "buy success", event.timestamp)
      }
    )
    //输出匹配到的正常数据，按照pattern的匹配条件
    resultDstream.print()
    //输出匹配到的超时数据
    val timeoutDstram: DataStream[OrderResult] = resultDstream.getSideOutput(outputTag)
    timeoutDstram.print()

    environment.execute("Order TimeOut Job")

  }

}
