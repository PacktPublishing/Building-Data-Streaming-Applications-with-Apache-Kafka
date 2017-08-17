

import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.apache.log4j.Logger
import java.util._


object DemoConsumer {

  @throws[Exception]
  def main(args: Array[String]) {
    val topic: String = "test1"
    val topicList: List[String] = new ArrayList[String]
    topicList.add(topic)
    val consumerProperties: Properties = new Properties
    consumerProperties.put("bootstrap.servers", "10.200.99.197:6667")
    consumerProperties.put("group.id", "Demo_Group")
    consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    consumerProperties.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    consumerProperties.put("enable.auto.commit", "true")
    consumerProperties.put("auto.commit.interval.ms", "1000")
    consumerProperties.put("session.timeout.ms", "30000")
    val demoKafkaConsumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](consumerProperties)
    demoKafkaConsumer.subscribe(topicList)
    println("Subscribed to topic " + topic)
    val i: Int = 0
    try
        while (true) {
          val records: ConsumerRecords[String, String] = demoKafkaConsumer.poll(2)
          import scala.collection.JavaConversions._
          for (record <- records) {
            println("offset = " + record.offset + "key =" + record.key + "value =" + record.value)
            System.out.print(record.value)
          }
          //TODO : Do processing for data here
          demoKafkaConsumer.commitAsync(new OffsetCommitCallback() {
            def onComplete(map: Map[TopicPartition, OffsetAndMetadata], e: Exception) {
            }
          })
        }

    catch {
      case ex: Exception => {
        //TODO : Log Exception Here
      }
    } finally try
      demoKafkaConsumer.commitSync()
    finally demoKafkaConsumer.close()
  }
}
S