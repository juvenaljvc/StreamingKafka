import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.httpclient.auth._
import com.twitter.hbc.core.{Client, Constants}
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.streaming.Minutes

import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, TimeUnit}
import java.util.Collections
import scala.collection.JavaConverters._
import twitter4j._
import twitter4j.conf.{Configuration, ConfigurationBuilder}
import twitter4j.auth.OAuthAuthorization
import org.apache.spark.streaming.twitter.TwitterUtils

import java.util.logging.{LogManager, Logger}
class TwitterKafkaStreaming {

  private val trace_kafka : Logger = LogManager.getLogManager.getLogger("Log_Console")
  private def twitterOAuthConf(CONSUMER_KEY : String, CONSUMER_SECRET : String,ACCES_TOKEN : String, TOCKEN_SECRET : String) : ConfigurationBuilder = {
    val twitterConf : ConfigurationBuilder = new ConfigurationBuilder()
    twitterConf
      .setJSONStoreEnabled(true)
      .setDebugEnabled(true)
      .setOAuthConsumerKey(CONSUMER_KEY)
      .setOAuthConsumerSecret(CONSUMER_SECRET)
      .setOAuthAccessToken(ACCES_TOKEN)
      .setOAuthAccessTokenSecret(TOCKEN_SECRET)
    return twitterConf
  }

  /**
   * Ce client Hosebird permet de clollecter les données(tweets) streaming en provenance de twitter et sont envoy ées dans un ou plusieurs topics suur kafka
   *
   * @param CONSUMER_KEY          : la clé du consommateur pour l'authentification OAuth
   * @param CONSUMER_SECRET       : Le secret du consommateur pour l'authentification OAuth
   * @param ACCES_TOKEN           : Le token d'accés pour l'authentification OAuth
   * @param TOCKEN_SECRET         : le token secret pour l'authentification OAuth
   * @param liste_hashtags        : les listes des hashtags des tweets dont on souhaite collecter
   * @param duree                 : la duree de deplay avant chaque collecte
   * @param kafkaBootStrapServers : la liste d'adresse IP et leur port des agents du cluster kafka
   * @param topic_name            le(s) topic(s) dans le(s)quel(s) est(sont) stocké(s) les tweets collecter
   */
  def producerTwitterKafkaHbc(CONSUMER_KEY: String, CONSUMER_SECRET: String, ACCES_TOKEN: String, TOCKEN_SECRET: String,
                              liste_hashtags: String, duree: Int, kafkaBootStrapServers: String, topic_name: String): Unit = {


    val queue: BlockingQueue[String] = new LinkedBlockingQueue[String](10000)

    val auth: Authentication = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCES_TOKEN, TOCKEN_SECRET)

    val endpoint: StatusesFilterEndpoint = new StatusesFilterEndpoint()
    endpoint.trackTerms(Collections.singletonList(liste_hashtags))
    endpoint.trackTerms(List(liste_hashtags).asJava) // Collections.singletonList()

    val construteurClientHbc: ClientBuilder = new ClientBuilder()
      .hosts(Constants.STREAM_HOST)
      .authentication(auth)
      .gzipEnabled(true)
      .processor(new StringDelimitedProcessor(queue))

    val clientHbc: Client = construteurClientHbc.build()
    try {
      clientHbc.connect()

      while (!clientHbc.isDone) {
        val tweets: String = queue.poll(duree, TimeUnit.SECONDS)
        KafkaStreaming.getKafkaProducer(kafkaBootStrapServers, topic_name, null, tweets)
        println("message Twitter :" + tweets)
      }
    } catch {
      case ex: Exception =>
        println(ex.printStackTrace())
    } finally {
      clientHbc.stop()
    }
  }


  def producerTwitter4JKafka(CONSUMER_KEY: String, CONSUMER_SECRET: String, ACCES_TOKEN: String, TOCKEN_SECRET: String,
                             liste_hashtags: String, duree : Int,  kafkaBootStrapServers: String, topic_name: String): Unit = {
    val queue : BlockingQueue[Status] = new LinkedBlockingQueue[Status](10000)
    val twitterConf : ConfigurationBuilder = twitterOAuthConf(CONSUMER_KEY, CONSUMER_SECRET, ACCES_TOKEN, TOCKEN_SECRET)

      val listener = new StatusListener {
      override def onStatus(status: Status): Unit = {

        trace_kafka.info("Evènement d'ajout de tweets détecté. Tweet Complet " + status.getText)
        queue.put(status)
        //1er méthode tout est sauvegardé directement dans les logs Kafka
        KafkaStreaming.getKafkaProducer(kafkaBootStrapServers, topic_name, null, status.getText)
      }

      override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = {}

      override def onTrackLimitationNotice(i: Int): Unit = {}

      override def onScrubGeo(l: Long, l1: Long): Unit = {}

      override def onStallWarning(stallWarning: StallWarning): Unit = {}

      override def onException(e: Exception): Unit = {
        trace_kafka.severe("Erreur générée par twitter : "+ e.printStackTrace())
      }
    }
    val twitterStream : TwitterStream= new TwitterStreamFactory(twitterConf.build()).getInstance()
    twitterStream.addListener(listener)
    //twitterStream.sample() //on récupère tous les tweets
    val query = new FilterQuery().track(liste_hashtags) //requète contient les éléments recherchés.
    twitterStream.filter(query)


    while(true){
      val  tweet : Status = queue.poll(duree, TimeUnit.SECONDS)
      KafkaStreaming.getKafkaProducer(kafkaBootStrapServers, topic_name, null, tweet.getText)
    }

  }


  /**
   *
   * @param CONSUMER_KEY
   * @param CONSUMER_SECRET
   * @param ACCES_TOKEN
   * @param TOCKEN_SECRET
   * @param liste_hashtags
   * @param duree
   * @param kafkaBootStrapServers
   * @param topic_name
   */
  def producerTwitterKafkaSpark(CONSUMER_KEY: String, CONSUMER_SECRET: String, ACCES_TOKEN: String, TOCKEN_SECRET: String,
                                liste_hashtags: String, duree : Int,  kafkaBootStrapServers: String, topic_name: String, env : Boolean) : Unit = {

    val oauth = new OAuthAuthorization(twitterOAuthConf(CONSUMER_KEY, CONSUMER_SECRET, ACCES_TOKEN, TOCKEN_SECRET).build())


    val clientStreamingTwitter = TwitterUtils.createStream(SparkBigData.getSparkStreamingContext(true, 15), Some(oauth))

    val tweetsmSG = clientStreamingTwitter.flatMap(status => status.getText)
    val tweetsComplet = clientStreamingTwitter.flatMap(status => status.getText+" "+ status.getContributors + " "+ status.getLang)
    val tweetFr = clientStreamingTwitter.filter(status => status.getLang == "Fr")
    val hashtag = clientStreamingTwitter.flatMap(status => status.getText.split(" ").filter(status => status.startsWith("#")))
    val hashtagFr = tweetFr.flatMap(status => status.getText.split(" ").filter(status => status.startsWith("#")))
    val hashtagFrCount = hashtagFr.window(Minutes(3))

    //tweetsmSG.saveAsTextFiles("Tweets") //persisté dans un fichier externe
    tweetsmSG.foreachRDD{
      (tweetsRDD, time) => if(!tweetsRDD.isEmpty()) {
        tweetsRDD.foreachPartition {
        partitionOfTweets =>
          val producerKafka = new KafkaProducer[String, String](KafkaStreaming.getKafkaProducerParams(kafkaBootStrapServers))
          partitionOfTweets.foreach{
            tweetEvent =>
              val record = new ProducerRecord[String, String](topic_name, tweetEvent.toString)
              producerKafka.send(record)

        }
          producerKafka.close()
      }

    }
  }
    try {
      tweetsComplet.foreachRDD{
        tweetsRDD =>
          if (!tweetsRDD.isEmpty()){
            tweetsRDD.foreachPartition{
              tweetsPartition => tweetsPartition.foreach{
                tweets =>
                  KafkaStreaming.getKafkaProducer(kafkaBootStrapServers,topic_name, null, tweets.toString)
              }
            }
          }

          KafkaStreaming.getKafkaProducer(kafkaBootStrapServers,""," ","").close()
      }
    } catch {
      case ex : Exception => trace_kafka.severe("  " + ex.printStackTrace())

    }

    SparkBigData.getSparkStreamingContext(env,15).start()
    SparkBigData.getSparkStreamingContext(env,15).awaitTermination()
  }
}