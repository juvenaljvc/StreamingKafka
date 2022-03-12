import com.fasterxml.jackson.databind.node.JsonNodeFactory
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord, KafkaProducer, RecordMetadata, Callback}
import org.apache.log4j.{LogManager, Logger}



import java.util.Properties

object ProducerKafkaPro {

  def main(args: Array[String]): Unit = {
    //var i = 1
    val str : String = null
    println(s"Début MAin")
    for(i <- 1 to 10){
      //var recordProducer : ProducerRecord[String,String] = getJson("orderline", str);
      //println(s"Record ${recordProducer.toString}")
      try {
        kafkaProducerPro("localhost:9092", getJson("orderline", str))
      } catch {
        case ex :Exception =>
          println(s"  ${ex.printStackTrace()}")
      }

    }
  }

  private val trace_kafka : Logger = LogManager.getLogger("Logger_Console")

  def getJson (topic_name : String,cle : String) : ProducerRecord[String,String]={
    val objetJson = JsonNodeFactory.instance.objectNode()
    objetJson.put("orderId", "10")
    objetJson.put("customerId", "150")
    objetJson.put("campaignId", "30")
    objetJson.put("orderDate", "10/04/2020")
    objetJson.put("city", "Paris")
    objetJson.put("state", "RAS")
    objetJson.put("zipcode", "75000")
    objetJson.put("paymentType", "CB")
    objetJson.put("totalPrice", "30")
    objetJson.put("numOrderLines", 200)
    objetJson.put("numUnit", 10)

    var recordProducer : ProducerRecord[String,String] =  null
    if (cle == null){
      recordProducer = new ProducerRecord[String,String](topic_name, objetJson.toString)
    } else {
      recordProducer = new ProducerRecord[String,String](topic_name,cle, objetJson.toString)
    }

    recordProducer

  }

  def kafkaProducerParamsPro (kafkaBootStrapServers : String) : Properties = {
    val props : Properties = new Properties()

    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootStrapServers)
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384")
    props.put(ProducerConfig.LINGER_MS_CONFIG, "100")
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip")
    //props.put("security.protocol", "SASL_PLAINTEXT")

    //propriété pour rendre le producer Exactly once
    //props.put(ProducerConfig.ACKS_CONFIG, "all")
    //props.put("min.insync.replicas", "2") // pour une cohérence éventuelle doit être inférieur ou égale au facteur de réplication du topic dans kafka
    //props.put("enable.idempotence", "true")
    //props.put(ProducerConfig.RETRIES_CONFIG, "3")
    //props.put("max.in.flight.requests.per.connection","2")


     props
  }

  def kafkaProducerPro( kafkaBootStrapServers : String, record: ProducerRecord[String, String]) : KafkaProducer[String, String] = {


    trace_kafka.info(s"instanciation d'une instance du producer Kafka aux serveurs ${kafkaBootStrapServers}")
    //println(s"instanciation d'une instance du producer Kafka aux serveurs ${kafkaBootStrapServers}")
    //println(s"Paramètres :  ${kafkaProducerParamsPro(kafkaBootStrapServers).toString}")
    lazy val producer = new KafkaProducer[String, String](kafkaProducerParamsPro(kafkaBootStrapServers))
    println(s"Producer : ${producer.toString}")
    //trace_kafka.info(s"message à publier dans le topic ${topic_name}, ${message}")
    //val recordProducer = getJson(topic_name, cle)

    try {

      trace_kafka.info("publication du message encours ...")
      producer.send(record, new Callback {
        override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
          if (e == null) {
            trace_kafka.info(s"Offset du message : ${recordMetadata.offset()}")
            trace_kafka.info(s"Topic du message : ${recordMetadata.topic()}")
            trace_kafka.info(s"Partition du message : ${recordMetadata.partition()}")
            trace_kafka.info(s"Heure d'enregistrement du message : ${recordMetadata.timestamp()}")

          }
        }
      })
      trace_kafka.info("message publié avec succés !:)")


    } catch {
      case ex: Exception =>
        trace_kafka.error(s"erreur dans la publication du message ${record.value()} dans le Log du topic ${record.topic()} dans kafka : ${ex.printStackTrace()}")
        trace_kafka.info(s"la liste des paramètres pour la connexion du producer Kafka sont : ${kafkaProducerParamsPro(kafkaBootStrapServers)}")
    } finally {
      println("Fin du producer")
    }

    producer

  }


}
