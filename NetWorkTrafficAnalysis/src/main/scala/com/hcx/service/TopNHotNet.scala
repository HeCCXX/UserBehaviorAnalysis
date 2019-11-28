package com.hcx.service

import java.sql.Timestamp

import com.hcx.dao.NetWorkLogCount
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

//自定义process function，统计访问量最大的url，排序输出
class TopNHotNet(topsize : Int) extends KeyedProcessFunction[Long,NetWorkLogCount,String]{
  //直接定义状态变量，懒加载
  lazy val urlstate : ListState[NetWorkLogCount] = getRuntimeContext.getListState(new ListStateDescriptor[NetWorkLogCount]("urlstate",classOf[NetWorkLogCount]))
  override def processElement(i: NetWorkLogCount, context: KeyedProcessFunction[Long, NetWorkLogCount, String]#Context, collector: Collector[String]): Unit = {
    //将每条数据保存到状态中
    urlstate.add(i)
    //注册一个定时器，windowend +10 秒触发
    context.timerService().registerEventTimeTimer(i.windowEnd + 10 *1000)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, NetWorkLogCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //将状态中所有url访问量获取到listbuffer中
    val listBuffer: ListBuffer[NetWorkLogCount] = ListBuffer()
    import scala.collection.JavaConversions._
    for (networklog <- urlstate.get()){
      listBuffer += networklog
    }
    //清空urlstate
    urlstate.clear()
    //按照访问量排序输出
    val netWorkLogCounts: ListBuffer[NetWorkLogCount] = listBuffer.sortBy(_.count)(Ordering.Long.reverse).take(topsize)

    //取出字符串返回，输出
    val resultStr = new StringBuilder
    resultStr.append("++++++++++++++++++\n")
    resultStr.append("时间： ").append(new Timestamp(timestamp - 10*1000)).append("\n")
    for (i <- listBuffer.indices) {
      val netWorkLogCount: NetWorkLogCount = listBuffer(i)
      resultStr.append("No").append(i+1).append(":")
        .append(" URL : ").append(netWorkLogCount.url)
        .append("流量 ： ").append(netWorkLogCount.count).append("\n")
    }
    resultStr.append("++++++++++++++++++++\n")
    out.collect(resultStr.toString())
    Thread.sleep(1000)
  }
}
