package com.github.esmusssein.unittest.kafka

import java.util.{Properties, UUID}

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.scalatest.{FlatSpec, Matchers}
import scala.collection.JavaConversions._

import scala.annotation.tailrec
import scala.concurrent.TimeoutException
import scala.concurrent.duration._

class TestKitSpec extends FlatSpec with TestKit with Matchers {

  @tailrec
  final def pollConsumerOffsetMessage(c: KafkaConsumer[String, String]): String = {
    c.poll(300).headOption match {
      case Some(rec) => rec.value
      case None => pollConsumerOffsetMessage(c)
    }
  }

  lazy val producer = {
    val props = new Properties
    props.put("bootstrap.servers", "localhost:6001")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    new KafkaProducer[String, String](props)
  }

  "TestKit" should "consume a message after subscribing the topic" in {
    producer.send(new ProducerRecord[String, String]("test-topic", "foo")).get()
    TestKit.subscribe("test-topic") {
      producer.send(new ProducerRecord[String, String]("test-topic", "bar"))
      TestKit.poll("test-topic") shouldBe "bar"
    }
  }

  it should "be able to consume multiple messages in a single test case" in {
    TestKit.subscribe("test-topic") {
      producer.send(new ProducerRecord[String, String]("test-topic", 0, "", "1")).get()
      producer.send(new ProducerRecord[String, String]("test-topic", 0, "", "2")).get()
      producer.send(new ProducerRecord[String, String]("test-topic", 0, "", "3")).get()
      TestKit.poll("test-topic") shouldBe "1"
      TestKit.poll("test-topic") shouldBe "2"
      TestKit.poll("test-topic") shouldBe "3"
    }
  }

  it should "throw TimeoutException if a message does't come during the timeout" in {
    TestKit.subscribe("test-topic") {
      val thrown = the[TimeoutException] thrownBy TestKit.poll("test-topic", 1.seconds)
      thrown.getMessage should be("No message received during 1000ms timeout for test-topic")
    }
  }

  it should "produce a message" in {
    val topic = UUID.randomUUID().toString
    val consumer = {
      val props = new Properties
      props.put("bootstrap.servers", "localhost:6001")
      props.put("group.id", s"${UUID.randomUUID()}")
      props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      props.put("auto.offset.reset", "earliest")
      new KafkaConsumer[String, String](props)
    }
    consumer.subscribe(List(topic))
    TestKit.send(topic, "foo")
    pollConsumerOffsetMessage(consumer) shouldBe "foo"
  }
}
