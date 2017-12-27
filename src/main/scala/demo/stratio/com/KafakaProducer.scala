package demo.stratio.com

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random
import collection.JavaConversions._
import com.google.gson.Gson

import scala.annotation.tailrec

object GraphQueryProducer extends App {


  val rnd = new Random()

  val producer = new KafkaProducer[String, String](mapAsJavaMap(Conf.kafkaParams))
  val t = System.currentTimeMillis()
  val gSon = new Gson

  infiniteLoop(0)
  @tailrec
  def infiniteLoop(nEvents:Long):Unit={
    val msg = gSon.toJson(Conf.Event(rnd.nextLong(), Conf.neoQuery, ""))
    println(msg)
    val data = new ProducerRecord[String, String](Conf.topic, nEvents.toString, msg)
    producer.send(data)
    Thread.sleep(100)
    infiniteLoop(nEvents+1L)
  }

  //System.out.println("sent per second: " + Cons.events * 1000 / (System.currentTimeMillis() - t))
  //producer.close()
}