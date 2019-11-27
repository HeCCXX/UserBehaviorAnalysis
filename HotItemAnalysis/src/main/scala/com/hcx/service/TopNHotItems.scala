package com.hcx.service

import java.sql.Timestamp

import com.hcx.dao.ItemViewCount
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * @Author HCX
 * @Description //TODO 求某个窗口中前N的热门商品，key为窗口时间戳，输出TopN的字符串集
 * @Date 13:51 2019-11-27
 *
 * @return
 * @exception
 **/

class TopNHotItems(topSize : Int) extends KeyedProcessFunction[Tuple,ItemViewCount,String]{
  private var itemState : ListState[ItemViewCount] = _


  override def open(parameters: Configuration): Unit = {super.open(parameters)
    //状态变量的名字和状态变量的类型
    val itemsStateDesc = new ListStateDescriptor[ItemViewCount]("itemState",classOf[ItemViewCount])
    //定义状态变量
    itemState = getRuntimeContext.getListState(itemsStateDesc)
  }

  override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
    //每条数据都保存到状态中
    itemState.add(i)
    //注册windowEnd + 1 的EventTime的  Timer ，当触发时，说明收齐了属于windowEnd窗口的所有数据
    context.timerService.registerEventTimeTimer(i.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //获取收到的商品点击量
    val allItems: ListBuffer[ItemViewCount] = ListBuffer()
    import scala.collection.JavaConversions._
    for (item <- itemState.get){
      allItems += item
    }
    //提前清除状态中的数据，释放空间
    itemState.clear()
    //按照点击量从大到小排序
    val sortedItems: ListBuffer[ItemViewCount] = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
    val result = new StringBuilder
    result.append("++++++++++++++++++\n")
    result.append("时间：").append(new Timestamp(timestamp -1 )).append("\n")
    for (i <- sortedItems.indices){
      val item: ItemViewCount = sortedItems(i)
      result.append("No").append(i+1).append(":")
        .append(" 商品ID : ").append(item.itemId)
        .append("  点击量 ： ").append(item.count).append("\n")
    }
    result.append("++++++++++++++++++\n")

    Thread.sleep(1000)
    out.collect(result.toString())
  }
}
