import KafkaStreaming._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges}

import java.util.logging.{LogManager, Logger}
object CommationStreaming {

  private val trace_kafka : Logger = LogManager.getLogManager.getLogger("Log_Console")

  val bootStrapServers : String = ""
  val consumerGroupID : String = ""
  val consumerReadOrder : String = ""
  val zookeeper : String = ""
  val kerberosName : String = ""
  val batchDuration : Int = 15
  val topics : Array[String] = Array("")
  var ssc : StreamingContext = null
  val  checkpointPath : String = null
  val schemaData = StructType( Array(
    StructField("Zipcode", IntegerType, true),
    StructField("ZipcodeType", StringType, true),
    StructField("City", StringType, true),
    StructField("State", StringType, true)
  ))

  /**
   *  Checkpointting avec Spark Streaming
   * @param checkpointPath : chemin d'enregistrement du checkpoint
   * @return : context spark streaming avec prise en compte du checkpoint
   */
  def faultTolerantSparkStreamingContext(checkpointPath : String) : StreamingContext = {
    val ssc2 = SparkBigData.getSparkStreamingContext(true, batchDuration)

    val kafkaStreams = getKafkaConsumer(bootStrapServers,consumerGroupID,consumerReadOrder,zookeeper,kerberosName,topics, ssc2)

    ssc2.checkpoint(checkpointPath)

    return  ssc2

  }
  def main (agrs: Array[String]): Unit ={

    if (checkpointPath == null){
      ssc = SparkBigData.getSparkStreamingContext(true, batchDuration)
    } else {
      ssc = StreamingContext.getOrCreate(checkpointPath, () => faultTolerantSparkStreamingContext(checkpointPath))
    }



    val kafkaStreams = getKafkaConsumer(bootStrapServers,consumerGroupID,consumerReadOrder,zookeeper,kerberosName,topics, ssc)
     //val dataStreams = kafkaStreams.map(record => record.value()) première façon de récupérer les données mais ne tient pas compte de la sémentique
    // Deuxième méthode récommandées
    kafkaStreams.foreachRDD{
      rddKafka => {
        if(!rddKafka.isEmpty()) {
          val  dataStreams = rddKafka.map(record => record.value())

          val ss = SparkSession.builder.config(rddKafka.sparkContext.getConf).enableHiveSupport().getOrCreate()
          import ss.implicits._
          val dfKafka = dataStreams.toDF("tweet_message")
          dfKafka.createOrReplaceGlobalTempView("kafka_events")

          // 1 ère méthode d'exploitation des données kafka et SQL
          val dfEventKafka = ss.sql("select * from kafka_events")
          dfEventKafka.show()

          // 2 ième méthode d'exploitation des données kafka et SQL
          val dfEventKafka2 = dfKafka.withColumn("tweet_message", from_json(col("tweet_message"), schemaData))
            .select(col("tweet_message.zipcode"))
            .select(col("tweet_message.zipCodeType"))
            .select(col("tweet_message.state"))
        }
      }

    }



    //Gestion des offsets
    kafkaStreams.foreachRDD{
      rddKafka => {
        if(!rddKafka.isEmpty()) {

          val offsets = rddKafka.asInstanceOf[HasOffsetRanges].offsetRanges

          val  dataStreams = rddKafka.map(record => record.value())

          for(o <- offsets){
            println(s"Le topic est : : ${o.topic}, la partition est : ${o.partition}, l'offset de début est : ${o.fromOffset} et l'offset de fin est : ${o.untilOffset}")
            trace_kafka.info(s"Le topic est : : ${o.topic}, la partition est : ${o.partition}, l'offset de début est : ${o.fromOffset} et l'offset de fin est : ${o.untilOffset}")
          }

          val ss = SparkSession.builder.config(rddKafka.sparkContext.getConf).enableHiveSupport().getOrCreate()
          import ss.implicits._
          val dfKafka = dataStreams.toDF("tweet_message")
          dfKafka.createOrReplaceGlobalTempView("kafka_events")

          // 1 ère méthode d'exploitation des données kafka et SQL
          val dfEventKafka = ss.sql("select * from kafka_events")
          dfEventKafka.show()

          // 2 ième méthode d'exploitation des données kafka et SQL
          val dfEventKafka2 = dfKafka.withColumn("tweet_message", from_json(col("tweet_message"), schemaData))
            .select(col("tweet_message.zipcode"))
            .select(col("tweet_message.zipCodeType"))
            .select(col("tweet_message.state"))

          //sémentique de livraison et traitement exactement une fois. Persistance des offset
          trace_kafka.info(s"Persistence des offsets encours ...")
          kafkaStreams.asInstanceOf[CanCommitOffsets].commitAsync(offsets)
          trace_kafka.info(s"Persistence des offsets terminée")
        }
      }

    }
    ssc.start()
    ssc.awaitTermination()



  }

}
