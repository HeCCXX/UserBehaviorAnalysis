package com.hcx.service

import java.lang

import com.hcx.dao.NetWorkLogCount
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @Author HCX
 * @Description //TODO 将累加器聚合后的数据封装成样例类输出
 * @Date 18:32 2019-11-27
 *
 * @return
 * @exception
 **/

class NetWindowResultFunction extends WindowFunction[Long,NetWorkLogCount,String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[NetWorkLogCount]): Unit = {
    val url = key
    val count: Long = input.iterator.next()
    out.collect(NetWorkLogCount(url,window.getEnd,count))
  }
}
