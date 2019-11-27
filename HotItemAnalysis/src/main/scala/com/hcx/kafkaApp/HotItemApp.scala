package com.hcx.kafkaApp

import com.hcx.dao.UserBehavior
import com.hcx.service.{CountAgg, TopNHotItems, WindowResultFunction}
import com.hcx.utils.KafkaUtil
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
/**
 * @Author HCX
 * @Description //TODO kafka 数据源
 * @Date 15:14 2019-11-27
 *
 * @return
 * @exception
 **/

object HotItemApp {

  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    environment.setParallelism(1)

    //隐式转换
    import org.apache.flink.api.scala._
    val kafkasink: FlinkKafkaConsumer[String] = new KafkaUtil().getConsumer("HotItem")
    val dstream: DataStream[String] = environment.addSource(kafkasink)

    //转换为样例类格式流
    val userDstream: DataStream[UserBehavior] = dstream.map(line => {
      val split: Array[String] = line.split(",")
      UserBehavior(split(0).toLong, split(1).toLong, split(2).toInt, split(3), split(4).toInt)
    })
    //指定时间戳和watermark
    val timestampsDstream: DataStream[UserBehavior] = userDstream.assignAscendingTimestamps(_.timestamp * 1000)
    //过滤用户点击行为的数据
    val clickDstream: DataStream[UserBehavior] = timestampsDstream.filter(_.behavior == "pv")
    //根据商品ID分组，并设置窗口一个小时的窗口，滑动时间为5分钟
    clickDstream.keyBy("itemId")
      .timeWindow(Time.minutes(60),Time.minutes(5))
      /**
       * preAggregator: AggregateFunction[T, ACC, V],
       * windowFunction: (K, W, Iterable[V], Collector[R]) => Unit
       * 聚合操作，AggregateFunction 提前聚合掉数据，减少state的存储压力
       * windowFunction  会将窗口中的数据都存储下来，最后一起计算
       */
      .aggregate(new CountAgg(),new WindowResultFunction())
      .keyBy("windowEnd")
      .process(new TopNHotItems(3))
      .print()


    environment.execute("Hot Item kafka Job")
  }

}
