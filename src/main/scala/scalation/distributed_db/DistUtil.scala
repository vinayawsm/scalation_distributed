package scalation.distributed_db

import scala.collection.mutable.ArrayBuffer
import scalation.columnar_db.Relation

/**
  * Created by vinay on 10/24/18.
  */
trait DistUtil {

    // Sequence of relations
    var rSeq: Seq[Relation] = Seq()

    // map of tables in database
    // params: name, index of "name" relation in rSeq
    var tableMap : Map[String, Int] = Map[String, Int]()

    // map of relations that are returned from workers
    // params: name, Array of relations returned from worker nodes
    var retTableMap : Map[String, scala.collection.mutable.ArrayBuffer[Relation]] = Map[String, ArrayBuffer[Relation]]()

    val ri = scala.util.Random

}
