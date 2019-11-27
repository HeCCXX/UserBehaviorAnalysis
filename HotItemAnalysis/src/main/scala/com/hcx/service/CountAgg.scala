package com.hcx.service

import com.hcx.dao.UserBehavior
import org.apache.flink.api.common.functions.AggregateFunction

/**
 * @Author HCX
 * @Description //TODO 累加器，窗口内每来一条数据加1
 * @Date 14:28 2019-11-27
 *
 * @return
 * @exception
 **/

class CountAgg extends AggregateFunction[UserBehavior,Long,Long]{
  override def createAccumulator(): Long = 0L

  override def add(in: UserBehavior, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}
