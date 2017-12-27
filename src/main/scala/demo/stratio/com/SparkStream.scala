package demo.stratio.com

import java.util.Date

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.neo4j.spark._
import com.google.gson.Gson

/**
  * Problem: Make a distribution of kafka's stream but the collect instruction is a full loss of calculations distribution
  * BUT WORKS :)
  */
object Stream {

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


    val lines = messages.map(x => {
      val gSon = new Gson
      gSon.fromJson(x.value, Conf.Event.getClass)
    })

    lines.foreachRDD(x => {
      val array = x.collect()
      array.foreach(x => {
        println()
        val graphQuery = "MATCH (n:User)-[r:OWN]->(m:Product) RETURN id(n) as source, id(m) as target, type(r) as value SKIP {_skip} LIMIT {_limit}"
        import org.apache.spark.graphx._
        import org.apache.spark.graphx.lib._
        val graph: Graph[Long, String] = neo.rels(graphQuery).partitions(8).batch(200).loadGraph
        val graph2 = PageRank.run(graph, 5)
        println(new Date()+ " ******* " + graph2.vertices.sortBy(_._2).first())
        println(x)
      })

    })

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}