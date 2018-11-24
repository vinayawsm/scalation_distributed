package scalation.dist_db

import akka.actor.{Actor, ActorRef, Props}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scalation.columnar_db.{Relation, TableGen}

/**
  * Created by vinay on 10/14/18.
  */


class RelDBWorker extends DistUtil with Actor {

    //    val routee: ActorRef = context.actorOf(Props[RelDBWorker], "routee")

    // DBWorker is implementation of worker nodes.
    // This receives messages from master and performs the operations from columnar_db
    def DBWorker() : Receive = {
        case createIn (name, colname, key, domain) =>
            if (!tableMap.exists(_._1 == name)) {
                val r = Relation (name, colname, Seq(), key, domain)
                rSeq += r
                tableMap += (name -> (rSeq.size - 1))
                sender() ! createReply (name, 0)
            }
            else sender() ! createReply (name, -1)

        case addIn (name, t) =>
            if (tableMap.exists(_._1 == name)) {
                rSeq(tableMap(name)).add(t)
                sender() ! addReply (name, 0)
            }
            else
                sender() ! addReply (name, -1)

        case materializeIn (name) =>
            rSeq(tableMap(name)).materialize()

        case tableGenIn (name, count) =>
            TableGen.popTable (rSeq(tableMap(name)), count)

        case selectIn (uc, r, p, rName) =>
            sender() ! relReply2 ("select_" + uc + "_" + rName, r.select(p._1, p._2), rName)

/*
        case projectIn (uc, name, cNames) =>
            if (tableMap.exists(_._1 == name))
                sender() ! relReply ("project_" + uc + "_" + tableMap(name), rSeq(tableMap(name)).project(cNames: _*))

        case unionIn (uc, r, q) =>
            if (tableMap.exists(_._1 == r) && tableMap.exists(_._1 == q))
                sender() ! relReply ("union_" + uc + "_" + tableMap(r) + "_" + tableMap(q),
                    rSeq(tableMap(r)).union(rSeq(tableMap(q))))
*/

        case minusIn (uc, r, q, rName) =>
            sender() ! relReply2 ("minus_" + uc + "_" + rName, r.minus(q), rName)

        case productIn (uc, r, q, rName) =>
            sender() ! relReply2 ("product_" + uc + "_" + rName, r.product(q), rName)

        case joinIn (uc, r, q, rName) =>
            sender() ! relReply2 ("product_" + uc + "_" + rName, r.product(q), rName)

        case intersectIn (uc, r, q, rName) =>
            sender() ! relReply2 ("product_" + uc + "_" + rName, r.intersect(q), rName)

        // returns to sender the relation at index i
        // params: index of relation in rSeq and unique code
        case showIn (uc, name) =>
            sender() ! relReply ("show_" + uc + "_" + tableMap(name), rSeq(tableMap(name)))

        case nameAll =>
            tableMap.foreach(n => println(n._1))
    }

    override def receive: Receive = DBWorker()

}
