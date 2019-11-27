package com.hcx.utils

import java.io.FileInputStream
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.{BufferedSource, Source}
import scala.collection.JavaConversions._

/**
 * @Author HCX
 * @Description //TODO kafka工具，将本地资源文件读取发送到kafka，kafka完成一个实时源
 * @Date 15:51 2019-11-27
 *
 * @return
 * @exception
 **/

class KafkaUtil {

  private val properties = new Properties()
  private val path: String = Thread.currentThread().getContextClassLoader.getResource("config.properties").getPath
  properties.load(new FileInputStream(path))

  private val config: Map[String, String] = Map(
    "bootstrap.servers" -> "hadoop1:9092",
    "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
    "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"

  )


  def getConsumer(topicName :String): FlinkKafkaConsumer[String] ={
    new FlinkKafkaConsumer[String](topicName,new SimpleStringSchema(),properties)
  }

  def getProducer(): KafkaProducer[String,String] ={

    new KafkaProducer[String,String](config)
  }

}
object KafkaUtil{
  def main(args: Array[String]): Unit = {

    val producer: KafkaProducer[String,String] = new KafkaUtil().getProducer()
    val source: BufferedSource = Source.fromFile("E:\\JavaProject\\UserBehaviorAnalysis\\HotItemAnalysis\\src\\main\\resources\\UserBehavior.csv")
    for (line <- source.getLines()){
      println(line)
      producer.send(new ProducerRecord[String,String]("HotItem",line))
    }
    producer.close()
  }
}
