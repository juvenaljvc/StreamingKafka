import KafkaStreaming._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
//import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges}

import org.apache.log4j.{LogManager, Logger}

object IndicateurStreaming {

  private val bootStrapServers : String = ""
  private val consumerGroupID : String = ""
  private val consumerReadOrder : String = ""
  private val zookeeper : String = ""
  private val kerberosName : String = ""
  private val batchDuration : Int = 900
  private val topics : Array[String] = Array("")
  //private var ssc : StreamingContext = null
  private  val path_fichier_kpi ="/data-lake/"

  def main(args : Array[String]): Unit ={
    val ssc = SparkBigData.getSparkStreamingContext(true, batchDuration)

    val kafkaStreams = getKafkaConsumer(bootStrapServers,consumerGroupID,consumerReadOrder,zookeeper,kerberosName,topics, ssc)

    kafkaStreams.foreachRDD{
      rdd_kafka => {
        try{
          val event_kafka = rdd_kafka.map(e => e.value())
          val offsets_kafka = rdd_kafka.asInstanceOf[HasOffsetRanges].offsetRanges

          val ssession = SparkSession.builder().config(rdd_kafka.sparkContext.getConf).enableHiveSupport().getOrCreate()
          import ssession.implicits._

          val event_df = event_kafka.toDF("kafka_jsons")

          if(event_df.count() == 0 ){
            val  log_events = Seq(
              ("Aucun évènement n'a été receptionné dans le quart d'heure")
            ).toDF("libelé")
              .coalesce(1)
              .write
              .format("com.databricks.spark.csv")
              .mode(SaveMode.Overwrite)
              .save(path_fichier_kpi+"/logs_streaming.csv")
          } else {
            val df_parsed = getParseData(event_df, ssession)
            val df_kpi = getIndicateurComputed(ssession,df_parsed).cache()

            df_kpi.repartition(1)
              .write
              .format("com.databricks.spark.csv")
              .mode(SaveMode.Append)
              .save(path_fichier_kpi+"/indicateurs_streaming.csv")
          }
          kafkaStreams.asInstanceOf[CanCommitOffsets].commitAsync(offsets_kafka)
        } catch {
          case ex : Exception =>
            trace_kafka.error(s" Il  y a une erreur dans l'application ${ex.printStackTrace()}")

        }

      }

    }
    ssc.start()
    ssc.awaitTermination()

  }

  private val trace_kafka : Logger =  LogManager.getLogger("Log_Console")

  val  schemaData = StructType(Array(
    StructField("eventDate", StringType,true),
    StructField("id", StringType,false),
    StructField("text", StringType,true),
    StructField("lang", StringType,true),
    StructField("userId", StringType,false),
    StructField("name", StringType,false),
    StructField("screenName", StringType,true),
    StructField("location", StringType,true),
    StructField("followersCounnt", IntegerType,false),
    StructField("retweetCount", IntegerType,false),
    StructField("favoriteCount", IntegerType,false),
    StructField("zipCode", StringType,true),
    StructField("zipCodeType", StringType,true),
    StructField("city", StringType,true),
    StructField("state", StringType,true)
  ))

  //def getParseData(kafkaEventsDF : DataFrame, ss : SparkSession, colName : String, schemaData : StructType) : DataFrame = {
  def getParseData(kafkaEventsDF : DataFrame, ss : SparkSession) : DataFrame = {
     trace_kafka.info(s"Parsing  des json recus de kafka encours ...")
    import ss.implicits._

    val dfEvents = kafkaEventsDF.withColumn("kafka_json", from_json(col("kafka_json"), schemaData))
      .filter(col("kafka_json"+".lang") === lit("en") || col("kafka_json"+".lang") === lit("fr"))
      .select(
        col("kafka_json"+".eventDate"),
        col("kafka_json"+".id"),
        col("kafka_json"+".text"),
        col("kafka_json"+".lang"),
        col("kafka_json"+".userId"),
        col("kafka_json"+".name"),
        col("kafka_json"+".screenName"),
        col("kafka_json"+".location"),
        col("kafka_json"+".followersCounnt"),
        col("kafka_json"+".retweetCount"),
        col("kafka_json"+".favoriteCount"),
        col("kafka_json"+".zipCode"),
        col("kafka_json"+".zipCodeType"),
        col("kafka_json"+".city"),
        col("kafka_json"+".state")
      ).toDF()

    return dfEvents

  }

  def getIndicateurComputed(ss : SparkSession, dfEventsParsed : DataFrame) : DataFrame = {
    //Obtention du numéro de série d'une date
    // val serieDate = unix_timestamp(col("eventData"), "dd-mm-yyyy")
    //val dateEvent = from_unixtime(serieDate)
    trace_kafka.info(s"Calcul des indicateur en cours ...")
    import  ss.implicits._
    dfEventsParsed.createOrReplaceGlobalTempView("events_tweet")
    val dfIndicateur = ss.sql(
      """
        |SELECT
        |      t.eventDate,
        |      t.quartHeure,
        |      COUNT(id) OVER (PARTITION BY eventDate, quart_heure ORDER By eventDate, quart_heure) as tweetCount,
        |      SUM(t.bin_retweet) OVER (PARTITION BY t.eventDate, t.quartHeure ORDER BY t.eventDate, t.quartHeure) as retweetCount
        |    FROM
        |        (
        |           SELECT
        |                from_unixtime(unix_timestamp(event_date, 'yyyy-MM-dd'), 'yyyy-MM-dd') as eventDate,
        |                CASE
        |                  WHEN minute(eventDate) < 15 THEN CONCAT(concat(from_unixtime(unix_timestamp(cast(hour(eventDate) as STRING), 'HH'), 'HH'), ":", "00", ":","00"),
        |                    "-",concat(from_unixtime(unix_timestamp(cast(hour(eventDate) as STRING), 'HH'), 'HH'), ":", "14", ":","59"))
        |                  WHEN minute(eventDate) between 15 AND 29 THEN CONCAT(concat(from_unixtime(unix_timestamp(cast(hour(eventDate) as STRING), 'HH'), 'HH'), ":", "15", ":","00"),
        |                    "-",concat(from_unixtime(unix_timestamp(cast(hour(eventDate) as STRING), 'HH'), 'HH'), ":", "29", ":","59"))
        |                  WHEN minute(eventDate) between 30 AND 44 THEN CONCAT(concat(from_unixtime(unix_timestamp(cast(hour(eventDate) as STRING), 'HH'), 'HH'), ":", "30", ":","00"),
        |                    "-",concat(from_unixtime(unix_timestamp(cast(hour(eventDate) as STRING), 'HH'), 'HH'), ":", "44", ":","59"))
        |                  WHEN minute(eventDate) > 44 THEN CONCAT(concat(from_unixtime(unix_timestamp(cast(hour(eventDate) as STRING), 'HH'), 'HH'), ":", "45", ":","00"),
        |                    "-",concat(from_unixtime(unix_timestamp(cast(hour(eventDate) as STRING), 'HH'), 'HH'), ":", "59", ":","59"))
        |                END AS quartHeure,
        |                CASE
        |                  WHEN retweetCount > 0 THEN 1
        |                  ELSE 0
        |                END AS bin_retweet,
        |                id
        |           FROM events_tweet
        |        ) t ORDER BY t.quartHeure
        """).withColumn("Niveau_RT", round(lit(col("retweetCount") / col("tweetCount")) * lit(100), 2))
            .withColumn("eventDate", when(col("eventDate").isNull, current_timestamp()).otherwise(col("eventDate")))
            .select(
              col("eventDate").alias("Date de l'event"),
              col("quartHeure").alias("Quart d'heure de l'event"),
              col("tweetCount").alias("Nbre de tweet par quart d'heure"),
              col("retweetCount").alias("Nbre de retweet par quart d'heure"),
              col("Niveau_RT").alias("Niveau de retweet (en %)")
            )

    return  dfIndicateur
  }



}
