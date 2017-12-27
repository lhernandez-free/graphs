package demo.stratio.com

import java.util.Date

import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010._
import org.neo4j.spark._
import com.google.gson.Gson
import scala.util.parsing.json.JSON

/**
  * Problem: Make a distribution of kafka's stream but the collect instruction is a full loss of calculations distribution
  * BUT WORKS :)
  */
object SparkStream {

  def main(args: Array[String]) {

    val ssc = new StreamingContext(Conf.sparkConf.setAppName("StreamGraphProcessor"), Milliseconds(100))
    val neo = Neo4j(ssc.sparkContext)
    // Create direct kafka stream with brokers and topics
    val topics = Conf.topic
    val topicsSet = topics.split(",").toSet

    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, Conf.kafkaParams))

    val lines: DStream[String] = messages.map(x => {

      val requestJson = JSON.parseFull(x.value())
      requestJson.get.asInstanceOf[Map[String, Any]].get("neoQuery").get.asInstanceOf[String]
    })

    lines.foreachRDD(x => {
      val array: Array[String] = x.collect()
      array.foreach(query => {
        import org.apache.spark.graphx._
        import org.apache.spark.graphx.lib._
        val graph: Graph[Long, String] = neo.rels(query).partitions(8).batch(200).loadGraph
        val graph2 = PageRank.run(graph, 5)
        println(new Date() + " ******* " + graph2.vertices.sortBy(_._2).first())
        println()
      })

    })

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}