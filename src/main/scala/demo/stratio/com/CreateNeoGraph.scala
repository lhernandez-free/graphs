package demo.stratio.com

import org.neo4j.driver.v1.AuthTokens
import org.neo4j.driver.v1.GraphDatabase



object CreateNeoGraph {

  def main(args: Array[String]) {

    val driver = GraphDatabase.driver(Conf.neoUrl, AuthTokens.basic(Conf.neoUser, Conf.neoPass))

    val session = driver.session

    val scriptUsers = "WITH [\"Andres\",\"Wes\",\"Rik\",\"Mark\",\"Peter\",\"Kenny\",\"Michael\",\"Stefan\",\"Max\",\"Chris\"]" +
    "AS names FOREACH (r IN range(0,100000) | CREATE (:User {id:r, name:names[r % size(names)]+\" \"+r}))"

    val scriptProducts = "with [\"Mac\",\"iPhone\",\"Das Keyboard\",\"Kymera Wand\",\"HyperJuice Battery\","+
    "\"Peachy Printer\",\"HexaAirBot\",\"AR-Drone\",\"Sonic Screwdriver\",\"Zentable\",\"PowerUp\"] as names "+
      "foreach (r in range(0,50) | create (:Product {id:r, name:names[r % size(names)]+\" \"+r}));"

    val scriptEdges = "match (u:User),(p:Product)"+
    "with u,p "+
    // increase skip value from 0 to 4M in 1M steps
    "skip 1000000 "+
    "limit 5000000 "+
    "where rand() < 0.1 "+
    "with u,p "+
    "limit 100000 "+
    "merge (u)-[:OWN]->(p)"

    List(scriptUsers,scriptProducts,scriptEdges).foreach(session.run)

    session.close()
  }
}