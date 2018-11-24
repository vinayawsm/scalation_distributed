package scalation

import scala.reflect.ClassTag
import scalation.columnar_db.Relation
import scalation.columnar_db.TableObj.Row
import scalation.linalgebra.Vec

/**
  * Created by vinay on 11/12/18.
  */
package object dist_db {

    /** Boolean function that uses the value for the given column name (String)
      *  in the predicate (e.g., used by 'where' and 'filter')
      */
    type Predicate [T] = (String, T => Boolean)

    /** Boolean function that uses the values for two given column names (String, String)
      *  in the predicate (e.g., used by 'thetajoin')
      */
    type Predicate2 [T] = (String, String, (T, T) => Boolean)

    /** Indicates which relation and which column an aggregate is to be applied to
      */
    type AggFunction = (Relation, String) => Vec

    /** Aggregate column type has Aggregate Funtion, new column name and old column name
      */
    type AggColumn = (AggFunction, String, String)

    case class create (name: String, colname: Seq[String], key: Int, domain: String)
    case class createIn (name: String, colname: Seq[String], key: Int, domain: String)
    case class createReply (name: String, n: Int)

    case class add (name: String, t: Row)
    case class addIn (name: String, t: Row)
    case class addReply (name: String, n: Int)
    case class materialize (name: String)
    case class materializeIn (name: String)

    case class tableGen (name: String, count: Int)
    case class tableGenIn (name: String, count: Int)

    case class show (name: String)
    case class showIn (uc: Int, name: String)

    // def select [T : ClassTag] (cName: String, p: T => Boolean): Relation
    case class select [T: ClassTag] (name: String, p: Predicate[T], rName: String)
    case class selectIn [T: ClassTag] (uc: Int, r: Relation, p: Predicate[T], rName: String)

    // def project (cName: String*): Relation
    case class project (name: String, cNames: Seq[String], rName: String)
    case class projectIn (uc: Int, name: String, cNames: Seq[String])

    // def union (r2: Table): Relation
    case class union (name: String, name2: String, rName: String)
    case class unionIn (uc: Int, r: String, q: String)

    // def minus (r2: Table): Relation
    case class minus (name: String, name2: String, rName: String)
    case class minusIn (uc: Int, r: Relation, q: Relation, rName: String)

    // def product (r2: Table): Relation
    case class product (r: String, q: String, rName: String)
    case class productIn (uc: Int, r: Relation, q: Relation, rName: String)

    // def join (r2: Table): Table
    case class join (r: String, q: String, rName: String)
    case class joinIn (uc: Int, r: Relation, q: Relation, rName: String)

    // def intersect (_r2: Table): Relation
    case class intersect (r: String, q: String, rName: String)
    case class intersectIn (uc: Int, r: Relation, q: Relation, rName: String)

    case class relReply (id: String, r: Relation)
    case class relReply2 (id: String, r: Relation, rName: String)

    case class delete (name: String)

    case object nameAll

}
