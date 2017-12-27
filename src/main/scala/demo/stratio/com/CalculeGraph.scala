package demo.stratio.com

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.neo4j.spark._
import com.google.gson.Gson

object KafkaStreaming {

  def main(args: Array[String]) {

    val topics = Cons.topic


    val ssc = new StreamingContext(Cons.sparkConf.setAppName("StreamGraphProcessor"), Milliseconds(100))
    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet

    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, Cons.kafkaParams))


    val lines = messages.map(x => {
      val gson = new Gson
      gson.fromJson(x.value, Cons.Event.getClass)
    })

    lines.foreachRDD(x => {
      val array = x.collect()
      array.foreach(x => {
        println()
        val graphQuery = "MATCH (n:User)-[r:OWN]->(m:Product) RETURN id(n) as source, id(m) as target, type(r) as value SKIP {_skip} LIMIT {_limit}"
        val neo = Neo4j(ssc.sparkContext)
        import org.apache.spark.graphx._
        import org.apache.spark.graphx.lib._
        val graph: Graph[Long, String] = neo.rels(graphQuery).partitions(8).batch(200).loadGraph
        val graph2 = PageRank.run(graph, 5)
        println("*******" + graph2.vertices.sortBy(_._2).first())
        println(x)
      })

    })

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}