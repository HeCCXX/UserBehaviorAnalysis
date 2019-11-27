package com.hcx.service

import com.hcx.dao.ItemViewCount
import org.apache.flink.api.java.tuple.{Tuple,Tuple1}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class WindowResultFunction extends WindowFunction[Long,ItemViewCount,Tuple,TimeWindow]{
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0
    val count: Long = input.iterator.next()
    out.collect(ItemViewCount(itemId,window.getEnd,count))
  }
}
