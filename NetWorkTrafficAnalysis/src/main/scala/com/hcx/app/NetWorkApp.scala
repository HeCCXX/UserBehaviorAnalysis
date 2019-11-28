package com.hcx.app
import java.text.SimpleDateFormat

import com.hcx.dao.NetWorkLog
import com.hcx.service.{NetCountAgg, NetWindowResultFunction, TopNHotNet}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
object NetWorkApp {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    environment.setParallelism(1)

    val dstream: DataStream[String] = environment.readTextFile("E:\\JavaProject\\UserBehaviorAnalysis\\NetWorkTrafficAnalysis\\src\\main\\resources\\network.log")

    import org.apache.flink.api.scala._
    val logdstream: DataStream[NetWorkLog] = dstream.map(line => {
      val split: Array[String] = line.split(" ")
      val format = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
      val timestamp: Long = format.parse(split(3)).getTime
      NetWorkLog(split(0), split(2), timestamp, split(5), split(6))
    })
    logdstream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[NetWorkLog](Time.milliseconds(1000)) {
      override def extractTimestamp(t: NetWorkLog): Long = {
        t.eventTime
      }
    })
      .keyBy(_.url)
      .timeWindow(Time.minutes(10),Time.seconds(5))
      .aggregate(new NetCountAgg,new NetWindowResultFunction)
      .keyBy(_.windowEnd)
      .process(new TopNHotNet(5))
      .print()

    environment.execute("Network Traffic Job")
  }

}
