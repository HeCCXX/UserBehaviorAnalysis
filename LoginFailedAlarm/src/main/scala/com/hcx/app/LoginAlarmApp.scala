package com.hcx.app

import com.hcx.dao.LoginEvent
import com.hcx.service.MatchFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * @Author HCX
 * @Description //TODO 恶意登录失败监控，当连续登录2次及以上警告
 * @Date 10:25 2019-11-28
 * @return
 * @exception
 **/

object LoginAlarmApp {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    import org.apache.flink.api.scala._
    //自定义少部分数据，用于测试
    val dstream: DataStream[LoginEvent] = environment.fromCollection(List(
      LoginEvent(1, "192.168.1.1", "400", 1558430842),
      LoginEvent(1, "192.168.1.2", "400", 1558430843),
      LoginEvent(1, "192.168.1.3", "400", 1558430844),
      LoginEvent(2, "192.144.144.12", "200", 1558430845)
    ))
    dstream.assignAscendingTimestamps(_.timestamp *1000)
      .filter(_.loginFlag == "400")
      .keyBy(_.userId)
      .process(new MatchFunction)
      .print()

    environment.execute("Login Failed Alarm")
  }

}
