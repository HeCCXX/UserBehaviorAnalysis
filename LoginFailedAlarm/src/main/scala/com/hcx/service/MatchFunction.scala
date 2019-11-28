package com.hcx.service

import com.hcx.dao.LoginEvent
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

class MatchFunction extends KeyedProcessFunction[Long,LoginEvent,String]{

  //定义状态变量
  lazy val alarmState:ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("alarmState",classOf[LoginEvent]))

  override def processElement(i: LoginEvent, context: KeyedProcessFunction[Long, LoginEvent, String]#Context, collector: Collector[String]): Unit = {
    //将获取的数据增加到状态变量中
    alarmState.add(i)

    //注册定时器，触发时间为2秒
    context.timerService().registerEventTimeTimer(i.timestamp + 2 *1000)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, String]#OnTimerContext, out: Collector[String]): Unit = {
    val listBuffer: ListBuffer[LoginEvent] = ListBuffer()
    import scala.collection.JavaConversions._
    for (loginEvent <- alarmState.get()){
      listBuffer.add(loginEvent)
    }
    alarmState.clear()

    if (listBuffer.size > 1){
      out.collect("用户ID："+listBuffer.get(0).userId + "  在2秒内登录失败次数过多，请注意！")
    }
  }
}
