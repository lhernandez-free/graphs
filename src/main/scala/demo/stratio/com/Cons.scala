package demo.stratio.com

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf

object Cons {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  final val topic = "showGraph"
  final val events = 100000
  final val neoUser = "neo4j"
  final val neoPass = "1"
  final val neoUrl = "bolt://localhost:7687"
  final val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:39092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "graphProcessors",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean),
    "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
    "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
  )
  val sparkConf = new SparkConf()
    .setAppName("Unnamed")
    .setMaster("local[2]")
    .set("spark.neo4j.bolt.user", neoUser)
    .set("spark.neo4j.bolt.password", neoPass)
    .set("spark.neo4j.bolt.url", neoUrl)

  final case class Event(userId: Long, neoQuery: String, algorithm: String)

}
