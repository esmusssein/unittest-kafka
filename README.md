# unittest-kafka
The easiest way to do unit testing for Scala with Kafka

## No need to run Kafka on your computer. Just follow the below.
```
class FooSpec extends FlatSpec with TestKit {
  import TestKit._

  it should "produce a message to topic FOO" in {
    subscribe("FOO") {
      Foo.produceKafkaMessage("hello world")
      poll("FOO") should be ("hello world")
    }
  }
  
  it should "receive a message from topic BAR" in {
    send("BAR", "hello world")
    ...
  }
}
```

## Further works
1. Publish its jar to a central repository.
2. Your opinion..