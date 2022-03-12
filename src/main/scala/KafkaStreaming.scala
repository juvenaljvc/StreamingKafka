


import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.streaming.kafka010._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer._
import org.apache.kafka.clients.producer.ProducerConfig._
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.kafka010.ConsumerStrategies._

import java.util.{Collections, Properties}
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.fasterxml.jackson.databind.node.ObjectNode


object KafkaStreaming {

  private val trace_kafka : Logger = LogManager.getLogger("Log_Console")

  var kafkaParams : Map[String, Object] = Map(null, null)
  var consumerKafka : InputDStream[ConsumerRecord[String, String]] = null

  /**
   *
   * @param kafkaBootStrapServers
   * @param kafkaConsumerGroupID
   * @param kafkaConsumerReadOder
   * @param kafkaZookeeper
   * @param kerberosName
   * @return
   */

  def getKafkaSparkConsumerParams(kafkaBootStrapServers : String, kafkaConsumerGroupID : String, kafkaConsumerReadOder : String,
                     kafkaZookeeper : String, kerberosName : String) :Map[String, Object] ={
    kafkaParams = Map(
      "bootstrap.server" -> kafkaBootStrapServers,
      "group.id" -> kafkaConsumerGroupID,
      "zookeeper.hosts" -> kafkaZookeeper,
      "auto.offset.reset" -> kafkaConsumerReadOder,
      "enable.auto.commit" -> (false:java.lang.Boolean),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "sasl.kerberos.service.name" ->kerberosName,
      "security.protocol" ->  SecurityProtocol.PLAINTEXT
    )

    return kafkaParams
  }


  /**
   *
   * @param kafkaBootStrapServers
   * @param kafkaConsumerGroupID
   * @param kafkaConsumerReadOder
   * @param kafkaZookeeper
   * @param kerberosName
   * @param kafkaTopics
   * @return
   */
  def getKafkaConsumer(kafkaBootStrapServers : String, kafkaConsumerGroupID : String,
                       kafkaConsumerReadOder : String, kafkaZookeeper : String,
                       kerberosName : String, kafkaTopics : Array[String], streamContext : StreamingContext) : InputDStream[ConsumerRecord[String, String]] = {

    try {
      //val ssc = SparkBigData.getSparkStreamingContext(env=true,duration_batch)
      kafkaParams = getKafkaSparkConsumerParams(kafkaBootStrapServers,kafkaConsumerGroupID,kafkaConsumerReadOder,kafkaZookeeper,kerberosName)
      consumerKafka = KafkaUtils.createDirectStream[String, String](
        streamContext,
        PreferConsistent,
        Subscribe[String,String](kafkaTopics, kafkaParams)
      )

    }catch {
      case ex : Exception =>
        trace_kafka.error(s" Erreur d'initialisation du consumer kafka ${ex.printStackTrace()}")
        trace_kafka.info(s"la liste des paramètres de connexion du cosumer kafka sont : ${kafkaParams}")
    }

    return consumerKafka


  }

  /**
   *
   * @param kafkaBootStrapServers
   * @param kafkaConsumerGroupID
   * @return
   */
  def getKafkaScalaConsumerParams(kafkaBootStrapServers : String, kafkaConsumerGroupID : String) : Properties={
    val props : Properties = new Properties()
    props.put( "bootstrap.server", kafkaBootStrapServers)
    props.put("groupe.id", kafkaConsumerGroupID)
    //props.put("zookeeper.hosts", kafkaZookeeper)
    props.put("auto.offset.reset", "latest") //kafkaConsumerReadOder)
    props.put("enable.auto.commit", "false")
    props.put("key.deserializer", classOf[StringDeserializer])
    props.put("value.deserializer", classOf[StringDeserializer])
    //props.put("sasl.kerberos.service.name", kerberosName)
    //props.put("security.protocol", SecurityProtocol.PLAINTEXT)
    return  props

  }

  def getClientConsumerKafka (kafkaBootStrapServers: String, kafkaConsumerGroupId : String, topic_list : Array[String]) : KafkaConsumer[String, String] = {
    trace_kafka.info("Instanciation d'un Consommateur Kafka ....")
    val consumer = new KafkaConsumer[String, String](getKafkaScalaConsumerParams(kafkaBootStrapServers, kafkaConsumerGroupId))
    try {
      consumer.subscribe(Collections.singletonList(topic_list.toString))

      while(true){
        val messages : ConsumerRecords[String, String] = consumer.poll(30) //poll(Duration.ofSeconds(30) methode plus récente
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
            trace_kafka.error("Erreur dans le commit des offset. Kafka n'a pas reçu le jeton de reconnaissance confconsumer.commitAsync()irmant la reception du message")
        }

      }
    } catch{
      case excep : Exception =>
           trace_kafka.error(" " + excep.printStackTrace())

    } finally {
      consumer.close()
    }


    return consumer
  }


  def getKafkaProducerParams (kafkaBootStrapServers : String) : Properties = {
    val props : Properties = new Properties()

    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks", "all")
    props.put("boostrap.server", kafkaBootStrapServers)
    props.put("security.protocol", "SASL_PLAINTEXT")

    return props
  }





  /**
   *
   * @param kafkaBootStrapServers
   * @param topic_name Le titre du sujet
   * @param message Le message à envoyer
   * @return
   */
  def getKafkaProducer( kafkaBootStrapServers : String, topic_name : String, cle : String, message : String) : KafkaProducer[String, String] = {
    trace_kafka.info(s"instanciation d'une instance du producer Kafka aux serveurs ${kafkaBootStrapServers}")
    lazy val producer = new KafkaProducer[String, String](getKafkaProducerParams(kafkaBootStrapServers))

    trace_kafka.info(s"message à publier dans le topic ${topic_name}, ${message}")
    var recordProducer : ProducerRecord[String,String] = null

    if (cle == null){
      recordProducer = new ProducerRecord[String,String](topic_name, message)
    } else {
      recordProducer = new ProducerRecord[String,String](topic_name,cle, message)
    }


    try{
      trace_kafka.info("publication du message encours ...")
      producer.send(recordProducer)
      trace_kafka.info("message publié avec succés !:)")
    } catch {
      case ex : Exception =>
        trace_kafka.error(s"erreur dans la publication du message ${message} dans le Log du topic ${topic_name} dans kafka : ${ex.printStackTrace()}")
        trace_kafka.info(s"la liste des paramètres pour la connexion du producer Kafka sont : ${getKafkaProducerParams(kafkaBootStrapServers)}")
    } finally {
      //producer.close()
    }

    return  producer
  }

  def getKafkaProducerParamsExactlyOnce (kafkaBootStrapServers : String) : Properties = {
    val props : Properties = new Properties()

    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootStrapServers)
    //propriété pour rendre le producer Exactly once
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put("min.insync.replicas", "2") // pour une cohérence éventuelle doit être inférieur ou égale au facteur de réplication du topic dans kafka
    props.put("enable.idempotence", "true")
    props.put(ProducerConfig.RETRIES_CONFIG, "3")
    props.put("max.in.flight.requests.per.connection","2")
    props.put("security.protocol", "SASL_PLAINTEXT")

    return props
  }

  def getJson (topic_name : String,cle : String) : ProducerRecord[String,String]={
    val objetJson = JsonNodeFactory.instance.objectNode()
    objetJson.put("orderId", "")
    objetJson.put("customerId", "")
    objetJson.put("campaignId", "")
    objetJson.put("orderDate", "")
    objetJson.put("city", "")
    objetJson.put("state", "")
    objetJson.put("zipcode", "")
    objetJson.put("paymentType", "CB")
    objetJson.put("totalPrice", "30")
    objetJson.put("numOrderLines", 200)
    objetJson.put("numUnit", 10)

    var recordProducer : ProducerRecord[String,String] =  null
    if (cle == null){
     return new ProducerRecord[String,String](topic_name, objetJson.toString)
    } else {
      return new ProducerRecord[String,String](topic_name,cle, objetJson.toString)
    }
  }

  /**
   *
   * @param kafkaBootStrapServers
   * @param topic_name Le titre du sujet
   * @param message Le message à envoyer
   * @return
   */
  def getKafkaProducerExactlyOnce( kafkaBootStrapServers : String, topic_name : String, cle : String, message : String) : KafkaProducer[String, String] = {
    trace_kafka.info(s"instanciation d'une instance du producer Kafka aux serveurs ${kafkaBootStrapServers}")
    lazy val producer = new KafkaProducer[String, String](getKafkaProducerParamsExactlyOnce(kafkaBootStrapServers))

    trace_kafka.info(s"message à publier dans le topic ${topic_name}, ${message}")
    val recordProducer = getJson(topic_name, cle)


    try{
      trace_kafka.info("publication du message encours ...")
      producer.send(recordProducer, new Callback {
        override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
          if(e == null){
            trace_kafka.info(s"Offset du message : ${recordMetadata.offset()}")
            trace_kafka.info(s"Topic du message : ${recordMetadata.topic()}")
            trace_kafka.info(s"Partition du message : ${recordMetadata.partition()}")
            trace_kafka.info(s"Heure d'enregistrement du message : ${recordMetadata.timestamp()}")

        }
        }
      })
      trace_kafka.info("message publié avec succés !:)")
    } catch {
      case ex : Exception =>
        trace_kafka.error(s"erreur dans la publication du message ${message} dans le Log du topic ${topic_name} dans kafka : ${ex.printStackTrace()}")
        trace_kafka.info(s"la liste des paramètres pour la connexion du producer Kafka sont : ${getKafkaProducerParams(kafkaBootStrapServers)}")
    } finally {
      println("Fin du producer")
      //producer.close()
    }

    return  producer
  }


}
