package scalation.dist_db

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.routing._

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scalation.columnar_db.TableObj.ucount
import scalation.columnar_db.{Relation, TableGen}
import scalation.dist_db.RelDBMasterTest4.actor
import scalation.linalgebra.Vec

/**
  * Created by vinay on 10/10/18.
  */


class RelDBMaster extends DistUtil with Actor {

    // creates a router with `numOfRoutees` routees
    val router: ActorRef = context.actorOf (RoundRobinPool (numOfRoutees).props(Props[RelDBWorker]), "router")

    // DBHandler handles all the DB messages on master side
    def DBHandler (): Receive = {

        // create new relation
        case create (name, colname, key, domain) =>
            updating += (name -> true)
            if (!tableMap.exists(_._1 == name)) {
                val r = Relation (name, colname, Seq(), key, domain)
                rSeq += r
                tableMap += (name -> (rSeq.size - 1))
            }
            updating += (name -> false)

        // reply for create message. recieves name of relation and status (-1 => already exists, else => created)
        case createReply (name: String, n: Int) =>
            if (n == -1) println ("Table " + name + " already exists")
            else println ("Table " + name + " created")

        // add new row to the table
        case add (name, t) =>
            updating += (name -> true)
            if (tableMap.exists(_._1 == name)) {
                rSeq(tableMap(name)).add(t)
            }
            updating += (name -> false)

        // reply for add message. recieves name of relation and status (-1 => table doesn't exists)
        case addReply (name: String, n: Int) =>
            if (n == -1) println ("Table " + name + " doesn't exists")
            else println ("Row added to table " + name)

        // materialize the table. must do after addition of table rows
        case materialize (name) =>
            updating += (name -> true)
            rSeq(tableMap(name)).materialize()
            updating += (name -> false)

        // creates same "random" tables in all the worker nodes (this is for worker nodes to have entire tables)
        case tableGen (name, count) =>
            updating += (name -> true)
            TableGen.popTable (rSeq(tableMap(name)), count)
            updating += (name -> false)

        case select (name, p, rName) =>
//            router ! Broadcast (selectIn (ri.nextInt(randomSeed), name, p))
            relCheck ("select", name)
            val (rid, rc, perN) = preComp (name, rName)
            for (i <- 0 until numOfRoutees)
                router ! selectIn (rid, subRelation (rSeq (tableMap(name)), i * perN,  math.min(rc, perN * (i + 1))), p, rName)

        // performed on master
        case project (name, cNames, rName) =>
            relCheck ("project", name)
            updating += (rName -> true)
            var pr = rSeq(tableMap(name)).project(cNames: _*)
            pr.name = rName
            rSeq += pr
            tableMap += (rName -> (rSeq.size - 1))
            updating += (rName -> false)

        // performed on master
        case union (name, name2, rName) =>
            relCheck ("union", name)
            relCheck ("union", name2)
            updating += (rName -> true)
            var ur = rSeq(tableMap(name)).union(rSeq(tableMap(name2)))
            ur.name = rName
            rSeq += ur
            tableMap += (rName -> (rSeq.size - 1))
            updating += (rName -> false)

        case minus (name, name2, rName) =>
            relCheck ("minus", name)
            relCheck ("minus", name2)
            val (rid, rc, perN) = preComp (name, rName)
            for (i <- 0 until numOfRoutees)
                router ! minusIn (rid, subRelation (rSeq (tableMap(name)), i * perN,  math.min (rc, perN * (i + 1))),
                                    rSeq (tableMap(name2)), rName)

        case product (name, name2, rName) =>
            relCheck ("product", name)
            relCheck ("product", name2)
            val (rid, rc, perN) = preComp (name, rName)
            for (i <- 0 until numOfRoutees)
                router ! productIn (rid, subRelation (rSeq (tableMap(name)), i * perN,  math.min (rc, perN * (i + 1))),
                                    rSeq (tableMap(name2)), rName)

        case join (name, name2, rName) =>
            relCheck ("join", name)
            relCheck ("join", name2)
            val (rid, rc, perN) = preComp (name, rName)
            for (i <- 0 until numOfRoutees)
                router ! joinIn (rid, subRelation (rSeq (tableMap(name)), i * perN,  math.min (rc, perN * (i + 1))),
                                    rSeq (tableMap(name2)), rName)

        case intersect (name, name2, rName) =>
            relCheck ("intersect", name)
            relCheck ("intersect", name2)
            val (rid, rc, perN) = preComp (name, rName)
            for (i <- 0 until numOfRoutees)
                router ! intersectIn (rid, subRelation (rSeq (tableMap(name)), i * perN,  math.min (rc, perN * (i + 1))),
                                    rSeq (tableMap(name2)), rName)

        // show the table
        // if you are getting updating warning, you may add sleep(time) before calling show()
        case show (name) =>
            relCheck ("show", name)
            rSeq (tableMap (name)).show()

        case relReply (id, r) =>
            // add elements to retTableMap -> do union of all the results -> (remove the entry from retTableMap)
            if (retTableMap.exists (_._1 == id))
                retTableMap (id) += r
            else
                retTableMap += (id -> ArrayBuffer(r))
            if (retTableMap (id).size == numOfRoutees) {
                var r: Relation = retTableMap (id)(0)
                for (i <- 1 until numOfRoutees)
                    r = r union retTableMap (id)(i)
                r.show ()
                retTableMap -= id
            }

        case relReply2 (id, r, rName) =>
            // add elements to retTableMap -> do union of all the results -> (remove the entry from retTableMap)
            if (retTableMap.exists (_._1 == id))
                retTableMap (id) += r
            else
                retTableMap += (id -> ArrayBuffer (r))
            if (retTableMap (id).size == numOfRoutees) {
                var r: Relation = retTableMap (id)(0)
                for (i <- 1 until numOfRoutees)
                    r = r union retTableMap (id)(i)
                r.name = rName
                rSeq += r
                tableMap += (rName -> (rSeq.size - 1))
                retTableMap -= id
                updating += (rName -> false)
            }

        case delete (name) =>
            relCheck ("delete", name)
            if (tableMap.exists (_._1 == name)) {
                val ind = tableMap (name)
                rSeq.remove (ind)
                tableMap -= name
                tableMap = tableMap ++ tableMap.filter(_._2 > ind).mapValues(_ - 1)
            }

        case nameAll =>
            tableMap.foreach (n => println(n._1))

    }

    override def receive: Receive = DBHandler()

    def preComp (name: String, rName: String): (Int, Int, Int) =
    {
        updating += (rName -> true)
        val rid = ri.nextInt (randomSeed)
        val rc = rSeq (tableMap(name)).rows
        val perN = math.ceil ((rc * 1.0) / numOfRoutees).toInt
        (rid, rc, perN)
    }

    def relCheck (f: String, name: String) =
    {
        if (updating(name)) {
            println (f + ": Table " + name + " is not updated yet")
            println ("Possible solution is to use Thread.sleep(n) before this query")
        }
    }

    def subRelation (r: Relation, se: Int, le: Int): Relation =
    {
        val newCol = (for (j <- r.col.indices) yield Vec.select (r.col(j), Seq.range(se, le))).toVector
        new Relation (r.name, r.colName, newCol, r.key, r.domain)
    }

}


// runMain scalation.dist_db.RelDBMasterTest
object RelDBMasterTest extends App {

    val actorSystem = ActorSystem("RelationDBMasterTest")
    val actor = actorSystem.actorOf(Props[RelDBMaster], "root")

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


    /*  This here, is not supposed to fail but it does!
        actor ! add("R1", Vector("abc7", 30, 180.0))

        -- You can add rows before materialize. Once it is done, you can only union other relations to extend this table.

        actor ! materialize ("R1")
    */

    actor ! show ("R2")
    actor ! show ("R1")

    actor ! select [Int] ("R1", ("Age", x => x < 25), "R1_Select")

//    Thread.sleep(3000)

    println("Select result:")
    actor ! show("R1_Select")
    //    println("Select done")

//    actor ! project ("R1", Seq("Name", "Age"), "R1_Project")

//    actor ! show ("R1_Project")
/*
    actor ! nameAll*/


    //    actor ! stop

    //    actorSystem.stop(actor)
    //    actorSystem.terminate()
}

// runMain scalation.dist_db.RelDBMasterTest2
object RelDBMasterTest2 extends App {
    val r1 = Relation("R1", Seq("Name", "Age", "Weight"), Seq(), 0, "SID")
    val r2 = Relation("R2", Seq("Name", "Age", "Weight"), Seq(), 0, "SID")

    TableGen.popTable(r1, 2)
    TableGen.popTable(r2, 2)

    r1.save()
    r2.save()
}


// runMain scalation.dist_db.RelDBMasterTest3
object RelDBMasterTest3 extends App {
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

// runMain scalation.dist_db.RelDBMasterTest4
object RelDBMasterTest4 extends App {
    val actorSystem = ActorSystem("RelationDBMasterTest")
    val actor = actorSystem.actorOf(Props[RelDBMaster], "root")

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

    actor ! union("R1", "R2", "R1uR2")
}

// runMain scalation.dist_db.RelDBMasterTest5
object RelDBMasterTest5 extends App {
    val actorSystem = ActorSystem("RelDBMasterTest5")
    val actor = actorSystem.actorOf(Props[RelDBMaster], "root")

    actor ! create("R1", Seq("Name", "Age", "Weight"), 0, "SID")
    actor ! create("R2", Seq("Name", "Height"), 0, "SD")

    actor ! tableGen ("R1", 4)
    actor ! tableGen ("R2", 5)

    actor ! show("R1")
}