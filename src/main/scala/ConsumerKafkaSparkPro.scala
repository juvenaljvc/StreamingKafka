
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.kafka010.ConsumerStrategies._

import java.util.{Collections, Properties}
import  org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import SparkBigData.getSparkStreamingContext


object ConsumerKafkaSparkPro {

  private val trace_kafka : Logger = LogManager.getLogger("Log_Console")

  def main(args: Array[String]): Unit={

    val paramsKafka : Map[String, Object] = kafkaConsumerSparkParams("localhost:9092","ced2", "earliest")
    val ssc : StreamingContext = getSparkStreamingContext(true,2)
    val topics : Array[String] = Array("orderline")

    try{

      val kafkaConsumer = kafkaConsumerSpark(ssc,topics,paramsKafka)



      println(s"Traitement des données dans le topic")

      kafkaConsumer.foreachRDD{
          rdd =>{
            try{
                val offsetKafka = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                val dataKafka = rdd.map(e => e.value())

                dataKafka.foreach{
                  l => println(l)
                }

                kafkaConsumer.asInstanceOf[CanCommitOffsets].commitAsync(offsetKafka)

            } catch {
              case e : Exception =>
                trace_kafka.error(s" Il  y a une erreur dans l'application ${e.printStackTrace()}")
            }
        }

      }

    } catch {
      case ex : Exception =>
        trace_kafka.error(s" Erreur d'initialisation du consumer kafka ${ex.printStackTrace()}")
        trace_kafka.info(s"la liste des paramètres de connexion du cosumer kafka sont : ${paramsKafka}")
    }

    ssc.start()
    ssc.awaitTermination()


  }

  def kafkaConsumerSparkParams(kafkaBootStrapServers : String, kafkaConsumerGroupID : String, kafkaConsumerReadOder : String)  :Map[String, Object] ={
    val kafkaParams = Map(
      "bootstrap.servers" -> kafkaBootStrapServers,
      "group.id" -> kafkaConsumerGroupID,
      "auto.offset.reset" -> kafkaConsumerReadOder,
      "enable.auto.commit" -> (false:java.lang.Boolean),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer]
      //"zookeeper.hosts" -> kafkaZookeeper, // Il faut ajouter aux paramètres de la fonction ceci "kafkaZookeeper : String"
      //"sasl.kerberos.service.name" ->kerberosName // il faut ajouter au paramètres de la fonction "kerberosName : String)"
      //"security.protocol" ->  SecurityProtocol.PLAINTEXT
    )

    kafkaParams
  }
/**
  def kafkaConsumerSpark( streamContext : StreamingContext, kafkaTopics : Array[String], kafkaParams : Map[String, Object]) : InputDStream[ConsumerRecord[String, String]] = {

    var consumerKafka : InputDStream[ConsumerRecord[String, String]] = null

    try {
      //val ssc = SparkBigData.getSparkStreamingContext(env=true,duration_batch)

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

    consumerKafka


  }
  */

def kafkaConsumerSpark( streamContext : StreamingContext, kafkaTopics : Array[String], kafkaParams : Map[String, Object]) : InputDStream[ConsumerRecord[String, String]] = {

  val consumerKafka : InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String] (
    streamContext,
    PreferConsistent,
    Subscribe[String, String](kafkaTopics, kafkaParams)
  )


  consumerKafka


}


}
