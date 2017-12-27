package demo.stratio.com

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.util.Random
import collection.JavaConversions._
import com.google.gson.Gson

object GraphQueryProducer extends App {


  val rnd = new Random()

  val producer = new KafkaProducer[String, String](mapAsJavaMap(Cons.kafkaParams))
  val t = System.currentTimeMillis()
  var nEvents = 0
  val gson = new Gson

  while (true) {

    val msg = gson.toJson(Cons.Event(rnd.nextLong(), "", ""))
    println(msg)
    val msg2 = gson.fromJson(msg,Cons.Event.getClass)
    println(msg2.toString)
    val data = new ProducerRecord[String, String](Cons.topic, nEvents.toString, msg.toString())
    producer.send(data)
    nEvents = nEvents + 1
    Thread.sleep(2000)
  }

  //System.out.println("sent per second: " + Cons.events * 1000 / (System.currentTimeMillis() - t))
  //producer.close()
}