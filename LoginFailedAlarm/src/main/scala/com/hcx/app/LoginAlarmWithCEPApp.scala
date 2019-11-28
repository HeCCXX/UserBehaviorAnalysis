package com.hcx.app

import com.hcx.dao.LoginEvent
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @Author HCX
 * @Description //TODO 恶意登录监控警告，使用cep实现
 * @Date 14:02 2019-11-28
 *
 * @return
 * @exception
 **/

object LoginAlarmWithCEPApp {

  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    environment.setParallelism(1)
    import org.apache.flink.api.scala._
    val dstream: DataStream[LoginEvent] = environment.fromCollection(List(
      LoginEvent(1, "192.168.0.1", "400", 1558430842),
        LoginEvent(1, "192.168.0.2", "400", 1558430843),
        LoginEvent(1, "192.168.0.3", "400", 1558430844),
        LoginEvent(2, "192.168.0.3", "400", 1558430845),
        LoginEvent(2, "192.168.10.10", "200", 1558430845)
    ))
    val keyStream: KeyedStream[LoginEvent, Long] = dstream.assignAscendingTimestamps(_.timestamp * 1000)
      .keyBy(_.userId)
    //定义匹配模式
    val loginPattern: Pattern[LoginEvent, LoginEvent] = Pattern.begin[LoginEvent]("begin").where(_.loginFlag == "400")
      .next("next").where(_.loginFlag == "400")
      .within(Time.seconds(2))
    //在数据流中匹配出定义好的模式
    val patternStream: PatternStream[LoginEvent] = CEP.pattern(keyStream,loginPattern)

    import  scala.collection.Map
    //select方法传入pattern select function，当检测到定义好的模式就会调用
    patternStream.select(
      (pattern : Map[String,Iterable[LoginEvent]]) =>
    {
      val event: LoginEvent = pattern.getOrElse("begin",null).iterator.next()

      (event.userId,event.ip,event.loginFlag)
    }
    )
      .print()

    environment.execute("Login Alarm With CEP")


  }

}
