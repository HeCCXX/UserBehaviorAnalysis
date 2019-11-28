package com.hcx.service

import com.hcx.dao.NetWorkLog
import org.apache.flink.api.common.functions.AggregateFunction

class NetCountAgg extends AggregateFunction[NetWorkLog,Long,Long]{
  override def createAccumulator(): Long = 0L

  override def add(in: NetWorkLog, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}
