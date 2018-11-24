package scalation.distributed_db

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.routing._

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scalation.columnar_db.{Relation, TableGen}

/**
  * Created by vinay on 10/10/18.
  */


class RelationDBMaster extends DistUtil with Actor {

    val numOfRoutees = 4
    val randomSeed = 1000000

    // creates a router with `numOfRoutees` routees
    val router: ActorRef = context.actorOf (RoundRobinPool (numOfRoutees).props(Props[RelationDBWorker]), "router")

    // DBHandler handles all the DB messages on master side
    def DBHandler (): Receive = {
        // create new relation
        case create (name, colname, key, domain) =>
            router ! Broadcast(createIn(name, colname, key, domain))

        // reply for create message. recieves name of relation and status (-1 => already exists, else => created)
        case createReply (name: String, n: Int) =>
            if (n == -1) println ("Table " + name + " already exists")
            else println ("Table " + name + " created")

        // add new row to the table
        case add (name, t) =>
            router ! addIn(name, t)

        // reply for add message. recieves name of relation and status (-1 => table doesn't exists)
        case addReply (name: String, n: Int) =>
            if (n == -1) println ("Table " + name + " doesn't exists")
            else println ("Row added to table " + name)

        // materialize the table. must do after addition of table rows
        case materialize (name) =>
            router ! Broadcast (materializeIn (name))

        case select (name, p) =>
            router ! Broadcast (selectIn (ri.nextInt(randomSeed), name, p))

        case project (name, p) =>
            router ! Broadcast (projectIn (ri.nextInt(randomSeed), name, p))

        case union (r, q) =>
            router ! Broadcast (unionIn (ri.nextInt(randomSeed), r, q))

        // Fails since the tables are split up in worker nodes and they are not inter communicated
        case minus (r, q) =>
            router ! Broadcast (minusIn (ri.nextInt(randomSeed), r, q))

        // Fails since the tables are split up in worker nodes and they are not inter communicated
        case product (r, q) =>
            router ! Broadcast (productIn (ri.nextInt(randomSeed), r, q))

        // Fails since the tables are split up in worker nodes and they are not inter communicated
        // Check implementation for parallel-join / distributed-join
        case join (r, q) =>
            router ! Broadcast (joinIn (ri.nextInt(randomSeed), r, q))

        // Fails since the tables are split up in worker nodes and they are not inter communicated
        case intersect (r, q) =>
             router ! Broadcast (intersectIn (ri.nextInt(randomSeed), r, q))

        // show the table
        case show (name) =>
            router ! Broadcast (showIn(ri.nextInt(randomSeed), name))

        case relReply (id, r) =>
            // add elements to retTableMap -> do union of all the results -> (remove the entry from retTableMap)
            if (retTableMap.exists(_._1 == id))
                retTableMap(id) += r
            else
                retTableMap += (id -> ArrayBuffer(r))
            if (retTableMap(id).size == numOfRoutees) {
                var r: Relation = retTableMap(id)(0)
                for (i <- 1 until numOfRoutees)
                    r = r union retTableMap(id)(i)
                r.show()
                retTableMap -= id
            }

        case nameAll =>
            tableMap.foreach(n => println(n._1))

    }

    override def receive: Receive = DBHandler()

}


// runMain scalation.distributed_db.RelationDBMasterTest
object RelationDBMasterTest extends App {

    val actorSystem = ActorSystem("RelationDBMasterTest")
    val actor = actorSystem.actorOf(Props[RelationDBMaster], "root")

    actor ! create("R1", Seq("Name", "Age", "Weight"), 0, "SID")

//    Thread.sleep(3000)

    actor ! add("R1", Vector("abc1", 22, 133.0))
    actor ! add("R1", Vector("abc2", 32, 143.2))
    actor ! add("R1", Vector("abc3", 23, 157.5))
    actor ! add("R1", Vector("abc4", 12, 173.6))
    actor ! add("R1", Vector("abc5", 62, 213.4))
    actor ! add("R1", Vector("abc6", 24, 143.0))

    actor ! create("R2", Seq("Name", "Height"), 0, "SD")

//    Thread.sleep(3000)

    actor ! add("R2", Vector("abc1", 155.0))
    actor ! add("R2", Vector("abc2", 167.2))
    actor ! add("R2", Vector("abc3", 173.6))
    actor ! add("R2", Vector("abc4", 163.1))
    actor ! add("R2", Vector("abc5", 178.7))
    actor ! add("R2", Vector("abc6", 164.4))


    actor ! materialize ("R1")
    actor ! materialize ("R2")

/*  This is not supposed to fail but it does!
    actor ! add("R1", Vector("abc7", 30, 180.0))

    actor ! materialize ("R1")
*/

    actor ! show ("R2")
    actor ! show ("R1")

    actor ! select [Int] ("R1", ("Age", x => x < 25))

    actor ! project ("R1", Seq("Name", "Age"))

    actor ! nameAll


//    actor ! stop

//    actorSystem.stop(actor)
//    actorSystem.terminate()
}

// runMain scalation.distributed_db.RelationDBMasterTest2
object RelationDBMasterTest2 extends App {
    val r1 = Relation("R1", Seq("Name", "Age", "Weight"), Seq(), 0, "SID")
    val r2 = Relation("R2", Seq("Name", "Age", "Weight"), Seq(), 0, "SID")

    TableGen.popTable(r1, 2)
    TableGen.popTable(r2, 2)

    r1.save()
    r2.save()
}


// runMain scalation.distributed_db.RelationDBMasterTest3
object RelationDBMasterTest3 extends App {
//    val actorSystem = ActorSystem("RelationDBMasterTest")
//    val actor = actorSystem.actorOf(Props[RelationDBMaster], "root")
//
//    actor ! create("R1", Seq("Name", "Age", "Weight"), 0, "SID")
//    actor ! create("R2", Seq("Name", "Height"), 0, "SD")

    val r1 = Relation("R1")
    val r2 = Relation("R2")

    var totalTime = 0.0
    val iter = 5

    r1.show()
    r2.show()
//    (r1 union r2).show()
    r1 union r2

    for (i <- 1 to iter) {
        val t1 = System.nanoTime()
        r1.union(r2)
        val t2 = System.nanoTime()
        totalTime += (t2 - t1)
        println((t2-t1)/1000000000.0)
    }
    println("avg: " + (totalTime)/iter/1000000000.0)

}

// runMain scalation.distributed_db.RelationDBMasterTest4
object RelationDBMasterTest4 extends App {
    val actorSystem = ActorSystem("RelationDBMasterTest")
    val actor = actorSystem.actorOf(Props[RelationDBMaster], "root")

    actor ! create("R1", Seq("Name", "Age", "Weight"), 0, "SID")
    actor ! create("R2", Seq("Name", "Height"), 0, "SD")

    val r1 = Relation("R1", Seq("Name", "Age", "Weight"), Seq(), 0, "SID")
    val r2 = Relation("R2", Seq("Name", "Height"), Seq(), 0, "SD")

    TableGen.popTable(r1, 3)
    TableGen.popTable(r2, 3)

    for (i <- 0 until r1.rows) actor ! add("R1", r1.row(i))
    for (i <- 0 until r2.rows) actor ! add("R2", r2.row(i))
    actor ! materialize("R1")
    actor ! materialize("R2")
    actor ! show("R1")
    actor ! show("R2")

    actor ! union("R1", "R2")
}