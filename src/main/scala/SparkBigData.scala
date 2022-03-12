import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkBigData {

  var ss : SparkSession = null

  var sparkConf : SparkConf = null

  /**
   * fonction qui initialise et instancie une session spark
   * @param env : c'est une variable  qui indique l'environnement sur lequel notre application est déployée
   *              si Env = True, alors l'application est local, sinon, elle est déployée sur un cluster
   *
   */

  def Session_Spark (env : Boolean = true) : SparkSession = {
    if(env) {
      System.setProperty("hadoop.home.dir", "/home/cedric/Hadoop")
      ss = SparkSession.builder
        .master(master = "local[*]")
        .config("sparc.sql.crossJoin.enable", "true")
        .enableHiveSupport()
        .getOrCreate()
    }else{
      ss = SparkSession.builder
        .appName(name="Mon application Spark")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("sparc.sql.crossJoin.enable", "true")
        .enableHiveSupport()
        .getOrCreate()
    }
    ss
  }

  /**
   * Cette fonction initialise le context streaming
   *
   * @param env 'est une variable  qui indique l'environnement sur lequel notre application est déployée
   *            si Env = True, alors l'application est local, sinon, elle est déployée sur un cluster
   * @param duration_batch détermine la durée de chaque batch à exécuter
   * @return la fonction renvoie en résultat une instance du contexte streaming
   */
  def getSparkStreamingContext (env : Boolean = true, duration_batch : Int) : StreamingContext = {
    if(env) {
      sparkConf = new SparkConf().setMaster("local[*]") //LocalHost[nbre:Int] spécifie le nombre de thread à traiter et "*" pour un nombre déterminé par la machine d'exécution.
        .setAppName("Mon application Spark")
    }else{
      sparkConf = new SparkConf().setAppName("Mon application Spark")
    }

    val ssc : StreamingContext = new StreamingContext(sparkConf, Seconds(duration_batch))

    ssc
  }

}
