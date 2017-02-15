package com.github.esmusssein.unittest.kafka

import java.util.{Properties, UUID}

import kafka.api.{FetchRequest, PartitionFetchInfo}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.concurrent.TimeoutException
import scala.concurrent.duration._

protected[this] case class TestKitConsumer(value: KafkaConsumer[String, String], groupId: String)
protected[this] case class TestKitProducer(value: KafkaProducer[String, String])

object TestKit {
  private val log = LoggerFactory.getLogger(classOf[TestKit])

  private implicit def toKafkaConsumer(consumer: TestKitConsumer): KafkaConsumer[String, String] = consumer.value
  private implicit def toKafkaProducer(producer: TestKitProducer): KafkaProducer[String, String] = producer.value

  private def setOffsetToHighWatermark[K, V](topic: String)(implicit consumer: TestKitConsumer): Unit = {
    val simpleConsumer = new SimpleConsumer("localhost", 6001, 1000*1000, 30*1000, consumer.groupId)
    try {
      val highWatermark = {
        val req = new FetchRequest(requestInfo = Map(TopicAndPartition(topic, 0) -> PartitionFetchInfo(0, 0)))
        val resp = simpleConsumer.fetch(req)
        resp.highWatermark(topic, 0)
      }
      log.info(s"high watermark for ${consumer.groupId} - $topic is $highWatermark")
      if (highWatermark >= 0) {
        consumer.seek(new TopicPartition(topic, 0), highWatermark)
      }
    } finally {
      simpleConsumer.close()
    }
  }

  def subscribe(topics: String*)(body: => Unit)(implicit consumer: TestKitConsumer): Unit = {
    consumer.assign(topics.map(t => new TopicPartition(t, 0)))
    topics.foreach(t => setOffsetToHighWatermark(t))
    try {
      body
    } finally {
      topics.foreach(t => setOffsetToHighWatermark(t))
      consumer.unsubscribe()
    }
  }

  def poll(topic: String, timeout: Duration = 30.seconds)(implicit consumer: TestKitConsumer): String = {
    consumer.poll(timeout.toMillis).find(_.topic == topic) match {
      case Some(rec) =>
        log.info(s"fetch a message from ${rec.topic()}, ${rec.partition()}, ${rec.offset()}")
        val t = rec.value
        consumer.seek(new TopicPartition(topic, rec.partition), rec.offset+1)
        t
      case None =>
        throw new TimeoutException(s"No message received during ${timeout.toMillis}ms timeout for $topic")
    }
  }

  def send(topic: String, value: String)(implicit producer: TestKitProducer): Unit = {
    val meta = producer.send(new ProducerRecord[String, String](topic, value)).get()
    log.info(s"Sent a message to ${meta.topic}, ${meta.partition}, ${meta.offset}")
  }
}

trait TestKit extends EmbeddedKafka with BeforeAndAfterAll { this: Suite =>

  private val log = LoggerFactory.getLogger(classOf[TestKit])

  implicit protected var testKitProducer: TestKitProducer = {
    val props = new Properties
    props.put("bootstrap.servers", "localhost:6001")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    TestKitProducer(producer)
  }
  implicit protected var testKitConsumer: TestKitConsumer = {
    val groupId = "KafkaTestKit-" + UUID.randomUUID().toString
    val props = new Properties
    props.put("bootstrap.servers", "localhost:6001")
    props.put("group.id", groupId)
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "earliest")
    props.put("enable.auto.commit", "false")
    val consumer = new KafkaConsumer[String, String](props)
    TestKitConsumer(consumer, groupId)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    log.info("Start zookeeper and kafka")
    EmbeddedKafka.start()
  }

  override def afterAll(): Unit = {
    log.info("Clean up zookeeper and kafka")
    testKitConsumer.value.close()
    testKitProducer.value.close()
    EmbeddedKafka.stop()
    super.afterAll()
  }
}
