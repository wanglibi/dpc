package com.cmsz.util

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

object KafkaProducer {

  def send(message:String):Unit={
    val topic = "test"
    val props = new Properties()
    props.put("bootstrap.servers", "nd-00.hdp:6667")
    props.put("key.serializer", classOf[StringSerializer])
    props.put("value.serializer", classOf[StringSerializer])
//    props.put("partitioner.class", classOf[HashPartitioner].getName)
    props.put("producer.type", "sync")
    props.put("batch.size", "1")
    props.put("acks", "all")
    val producer = new KafkaProducer[String,String](props)
    val message2 = new ProducerRecord[String, String](topic, "1", message)
    producer.send(message2)
    println("send:"+message2)
    producer.close()
  }

}
