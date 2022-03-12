//import com.fasterxml.jackson.databind.node.JsonNodeFactory
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer, CommitFailedException}
import java.util.Collections
import org.apache.log4j.{LogManager, Logger}
import scala.collection.JavaConverters._

import java.util.Properties
object ConsumerKafkaPro {

  private val trace_kafka : Logger = LogManager.getLogger("Logger_Console")

  def main(args: Array[String]): Unit={
    val groupId : String = "ced"
    kafkaConsumer("localhost:9092", groupId, "orderline")
  }

  /**
   *
   * @param kafkaBootStrapServers
   * @param kafkaConsumerGroupID
   * @return
   */

  def KafkaConsumerParams(kafkaBootStrapServers : String, kafkaConsumerGroupID : String) : Properties={
    val props : Properties = new Properties()
    props.put( ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootStrapServers)
    if(kafkaConsumerGroupID!= null) {
      props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaConsumerGroupID)
    }
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest") //kafkaConsumerReadOder)
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")

    //props.put("sasl.kerberos.service.name", kerberosName)
    //props.put("security.protocol", SecurityProtocol.PLAINTEXT)
    //props.put("zookeeper.hosts", kafkaZookeeper)

    props

  }

  /**
   *
   * @param kafkaBootStrapServers
   * @param kafkaConsumerGroupId
   * @param topic_list
   * @return
   */
  def kafkaConsumer (kafkaBootStrapServers: String, kafkaConsumerGroupId : String, topic_list : String) : KafkaConsumer[String, String] = {
    trace_kafka.info("Instanciation d'un Consommateur Kafka ....")
    val consumer = new KafkaConsumer[String, String](KafkaConsumerParams(kafkaBootStrapServers, kafkaConsumerGroupId))
    try {
      consumer.subscribe(Collections.singletonList(topic_list))
      //consumer.subscribe(topic_list)

      while(true){
        val messages : ConsumerRecords[String, String] = consumer.poll(3) //poll(Duration.ofSeconds(30) methode plus récente
        if(!messages.isEmpty) {
          trace_kafka.info("Nombre de messages collectés dans la fenêtre : "+ messages.count())
          for(message <- messages.asScala){
            println("Topic: " + message.topic()+
              "Key: " + message.key()+
              "Value: " + message.value()+
              "Offset: " + message.offset()+
              "Partition: " + message.partition()
            )
          }
        }
        try{
          consumer.commitAsync()
        } catch {
          case ex : CommitFailedException =>
            trace_kafka.error(s"Erreur dans le commit des offset. Kafka n'a pas reçu le jeton de reconnaissance confirmant la reception du message ${ex.printStackTrace()}")
        }

      }
    } catch{
      case excep : Exception =>
        trace_kafka.error(" " + excep.printStackTrace())

    } finally {
      consumer.close()
    }


     consumer
  }


}
