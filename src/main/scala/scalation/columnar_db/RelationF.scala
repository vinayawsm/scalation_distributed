
//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** @author  Yang Fan, John Miller
 *  @version 1.5
 *  @date    Thu Apr 26 12:12:38 EDT 2018
 *  @see     LICENSE (MIT style license file).
 */

package scalation.columnar_db

import scala.language.implicitConversions
import scala.collection.mutable.ArrayBuffer

import scalation.linalgebra._
import scalation.math.StrO.StrNum
import scalation.math.{Complex, Rational, Real}
import scalation.util.banner

import TableObj.ucount

//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `RelationF` companion object provides additional functions for the `RelationF`
 *  class, specifically a factory method to build a relation frame from a relation.
 */
object RelationF
{
    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Create a `RelationF` object using a `Relation` object.
     *  @param r  the relation to create the relation frame from
     */
    def apply (r: Relation): RelationF = new RelationF (r.name, r.colName, r.col, r.key, r.domain)

} // RelationF object


//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `RelationF` class create a relation/data frame by extending the `Relation` class,
 *  and provides high order functions such as map and reduce.
 *  FIX - should extend Tabular, not Relation
 *  @param name     the name of the relation
 *  @param colName  the names of columns
 *  @param col      the Scala Vector of columns making up the columnar relation
 *  @param key      the column number for the primary key (< 0 => no primary key)
 *  @param domain   an optional string indicating domains for columns (e.g., 'SD' = 'StrNum', 'Double')
 *  @param fKeys    an optional sequence of foreign keys - Seq (column name, ref table name, ref column position)
 */
class RelationF (name: String, colName: Seq [String], col: Vector [Vec],
                 key: Int = 0, domain: String = null, fKeys: Seq [(String, String, Int)] = null)
      extends Relation (name, colName, col, key, domain, fKeys)
{
    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Filter the `RelationF` object by predicate 'p'.
     *  FIX - what are the effects of this method ???
     *  @param  p  the predicate used to filter
     *  @tparam T  predicate type
     */
    def filter [T] (p: Predicate [T]*): RelationF =
    {
        var pos = ArrayBuffer [Int] ()

        for (i <- p.indices) {

            domain.charAt (colMap(p(i)._1)) match {
            case 'D' =>
                val pos1 = col(colMap(p(i)._1)).asInstanceOf [VectorD].filterPos (p(i)._2.asInstanceOf [Double => Boolean])
                if (i > 0) pos = pos intersect pos1 else pos ++= pos1
            case 'I' =>
                val pos1 = col(colMap(p(i)._1)).asInstanceOf [VectorI].filterPos (p(i)._2.asInstanceOf [Int => Boolean])
                if (i > 0) pos = pos intersect pos1 else pos ++= pos1
            case 'L' =>
                val pos1 = col(colMap(p(i)._1)).asInstanceOf [VectorL].filterPos (p(i)._2.asInstanceOf [Long => Boolean])
                if (i > 0) pos = pos intersect pos1 else pos ++= pos1
            case 'C' =>
                val pos1 = col(colMap(p(i)._1)).asInstanceOf [VectorC].filterPos (p(i)._2.asInstanceOf [Complex => Boolean])
                if (i > 0) pos = pos intersect pos1 else pos ++= pos1
            case 'Q' =>
                val pos1 = col(colMap(p(i)._1)).asInstanceOf [VectorQ].filterPos (p(i)._2.asInstanceOf [Rational => Boolean])
                if (i > 0) pos = pos intersect pos1 else pos ++= pos1
            case 'R' =>
                val pos1 = col(colMap(p(i)._1)).asInstanceOf [VectorR].filterPos (p(i)._2.asInstanceOf [Real => Boolean])
                if (i > 0) pos = pos intersect pos1 else pos ++= pos1
            case 'S' =>
                val pos1 = col(colMap(p(i)._1)).asInstanceOf [VectorS].filterPos (p(i)._2.asInstanceOf [StrNum => Boolean])
                if (i > 0) pos = pos intersect pos1 else pos ++= pos1
            case _ =>
                flaw ("filter", "predicate type not supported")
                null
            } // match

        } // for
        RelationF (selectAt (pos))
    } // filter

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Map a column to a new column according to function 'f'.
     *  @param  f        the transformation function to apply
     *  @param  colName  the column to apply map function
     *  @tparam T        the type of the column
     */
    def map [T <: Any] (f: ((T) => T), colName: String): RelationF =
    {
        var coli = col(colMap(colName))
        var ncol: Vector [Vec] = null

        Vec (coli, 0) match {
        case _: Int => 
            coli = coli.asInstanceOf [VectorI]
            val newcol = VectorI (
                for (i <- coli.indices) yield f(Vec (coli, i).asInstanceOf [T]).asInstanceOf [Int])
            ncol = Vector.fill(1)(newcol)
        case _: Complex =>
            coli = coli.asInstanceOf [VectorC]
            val newcol = VectorC (
                for (i <- coli.indices) yield f(Vec (coli, i).asInstanceOf [T]).asInstanceOf [Complex])
            ncol = Vector.fill(1)(newcol)
        case _: Double =>
            coli = coli.asInstanceOf [VectorD]
            val newcol = VectorD (
                for (i <- coli.indices) yield f(Vec (coli, i).asInstanceOf[T]).asInstanceOf[Double])
            ncol = Vector.fill(1)(newcol)
        case _: Long =>
            coli = coli.asInstanceOf [VectorL]
            val newcol = VectorL (
                for (i <- coli.indices) yield f(Vec (coli, i).asInstanceOf [T]).asInstanceOf [Long])
            ncol = Vector.fill(1)(newcol)
       case _: Rational =>
            coli = coli.asInstanceOf [VectorQ]
            val newcol = VectorQ (
                for (i <- coli.indices) yield f(Vec (coli, i).asInstanceOf [T]).asInstanceOf [Rational])
            ncol = Vector.fill(1)(newcol)
        case _: Real =>
            coli = coli.asInstanceOf [VectorR]
            val newcol = VectorR (
                for (i <- coli.indices) yield f(Vec (coli, i).asInstanceOf [T]).asInstanceOf [Real])
        case _: StrNum =>
            coli = coli.asInstanceOf [VectorS]
            val newcol = VectorS (
                for (i <- coli.indices) yield f(Vec (coli, i).asInstanceOf [T]).asInstanceOf [StrNum])
            ncol = Vector.fill(1)(newcol)
        case _ => println ("map: vector type not supported"); null
        } // match

        val cNames = Seq (colName)
        new RelationF (name + "_m_" + ucount (), cNames, ncol, 0, null)
    } // map

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Reduce on column using function 'f'.
     *  @param  f        the transformation function to apply
     *  @param  colName  the column to apply map function
     *  @tparam T        the type of the column
     */
    def reduce [T <: Any] (f: ((T, T) => T), colName: String): RelationF =
    {
        var coli = col(colMap(colName))
        var ncol: Vector [Vec] = null

        coli match {
        case _: VectorI =>
            var result = 0
            coli = coli.asInstanceOf [VectorI]
            for (i <- coli.indices)
                result = f(result.asInstanceOf [T], Vec (coli, i).asInstanceOf [T]).asInstanceOf [Int]
            val newcol = VectorI (result)
            ncol = Vector.fill(1)(newcol)
        case _: VectorC =>
            var result = Complex (0.0)
            coli = coli.asInstanceOf [VectorC]
            for (i <- coli.indices)
                result = f(result.asInstanceOf [T], Vec (coli, i).asInstanceOf[T]).asInstanceOf [Complex]
            val newcol = VectorC (result)
            ncol = Vector.fill(1)(newcol)
        case _: VectorD =>
            var result = 0:Double
            coli = coli.asInstanceOf [VectorD]
            for (i <- coli.indices)
                result = f(result.asInstanceOf [T], Vec (coli, i).asInstanceOf [T]).asInstanceOf [Double]
            val newcol = VectorD (result)
            ncol = Vector.fill(1)(newcol)
        case _: VectorL =>
            var result = 0:Long
            coli = coli.asInstanceOf [VectorL]
            for (i <- coli.indices)
                result = f(result.asInstanceOf [T], Vec (coli, i).asInstanceOf [T]).asInstanceOf [Long]
            val newcol = VectorL (result)
            ncol = Vector.fill(1)(newcol)
        case _: VectorQ =>
            var result = Rational(0)
            coli = coli.asInstanceOf [VectorQ]
            for (i <- coli.indices)
                result = f(result.asInstanceOf [T], Vec (coli, i).asInstanceOf [T]).asInstanceOf [Rational]
            val newcol = VectorQ (result)
            ncol = Vector.fill(1)(newcol)
        case _: VectorR =>
            var result = Real(0)
            coli = coli.asInstanceOf [VectorR]
            for (i <- coli.indices)
                result = f(result.asInstanceOf [T], Vec (coli, i).asInstanceOf [T]).asInstanceOf [Real]
            val newcol = VectorR (result)
            ncol = Vector.fill(1)(newcol)
        case _: VectorS =>
            var result = new StrNum("0")
            coli = coli.asInstanceOf [VectorS]
            for (i <- coli.indices)
                result = f(result.asInstanceOf [T], Vec (coli, i).asInstanceOf [T]).asInstanceOf [StrNum]
            val newcol = VectorS (result)
            ncol = Vector.fill(1)(newcol)
        case _ => println ("reduce: vector type not supported"); null
        } // match

      val cNames = Seq (colName)
      new RelationF (name + "_r_" + ucount (), cNames, ncol, 0, null)
    } // reduce

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Fold on column using function 'f'.
     *  @param  initvalue  the initial starting value of the fold
     *  @param  f          function to apply on the column
     *  @param  colName    column to fold on
     *  @tparam T          the type of the column
     */
    def fold [T <: Any] (initvalue: T, f: ((T, T) => T), colName: String): RelationF =
    {
        var coli = col(colMap(colName))
        var ncol: Vector[Vec] = null

        initvalue match {
        case _: Int =>
            var result = initvalue
            coli = coli.asInstanceOf [VectorI]
            for (i <- coli.indices)
                result = f(result, Vec (coli, i).asInstanceOf [T])
            val newcol = VectorI (result.asInstanceOf [Int])
            ncol = Vector.fill(1)(newcol)
        case _: Double =>
            var result = initvalue
            coli = coli.asInstanceOf [VectorD]
            for (i <- coli.indices)
                result = f(result, Vec (coli ,i).asInstanceOf [T])
            val newcol = VectorD (result.asInstanceOf [Double])
            ncol = Vector.fill(1)(newcol)
        case _: Long =>
            var sum = initvalue
            coli = coli.asInstanceOf [VectorL]
            for (i <- coli.indices)
                sum = f(sum, Vec (coli, i).asInstanceOf [T])
            val newcol = VectorL (sum.asInstanceOf [Long])
            ncol = Vector.fill(1)(newcol)
        case _: Complex =>
            var sum = initvalue
            coli = coli.asInstanceOf [VectorC]
            for (i <- coli.indices)
                sum = f(sum, Vec (coli, i).asInstanceOf [T])
            val newcol = VectorC (sum.asInstanceOf [Complex])
            ncol = Vector.fill(1)(newcol)
        case _: Rational =>
            var result = initvalue
            coli = coli.asInstanceOf [VectorQ]
            for (i <- coli.indices)
                result = f(result, Vec (coli, i).asInstanceOf [T])
            val newcol = VectorQ (result.asInstanceOf [Rational])
            ncol = Vector.fill(1)(newcol)
        case _: Real =>
            var result = initvalue
            coli = coli.asInstanceOf [VectorR]
            for (i <- coli.indices)
                result = f(result, Vec (coli, i).asInstanceOf [T])
            val newcol = VectorR (result.asInstanceOf [Real])
            ncol = Vector.fill(1)(newcol)
        case _: StrNum =>
            var result = initvalue
            coli = coli.asInstanceOf [VectorS]
            for (i <- coli.indices)
                result = f(result, Vec (coli, i).asInstanceOf [T])
            val newcol = VectorS (result.asInstanceOf [StrNum])
            ncol = Vector.fill(1)(newcol)
        case _ => println (":+ vector type not supported"); null
        } // match

        val cNames = Seq ("1")
        new RelationF (name + "_f_" + ucount (), cNames, ncol, 0, null)
    } // fold

} // RelationF class


//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `RelationFTest` object tests the `RelationF` class's map, reduce, fold, filter funtions.
 *  > runMain scalation.columnar_db.RelationFTest
 */
object RelationFTest extends App
{
    val professorR = RelationF (Relation ("professor",
        Seq ("pid", "name", "department", "title"),
        Seq (Vector [Any] (1, "jackson", "pharm", 4),
             Vector [Any] (2, "ken",     "cs",    2),
             Vector [Any] (3, "pan",     "pharm", 0),
             Vector [Any] (4, "yang",    "gis",   3),
             Vector [Any] (5, "zhang",   "cs",    0),
             Vector [Any] (6, "Yu",      "cs",    0)),
        0, "ISSI"))

    val professor = RelationF (professorR)

    professor.generateIndex ()

    banner ("professor")
    professor.show ()

    banner ("Test filter")
    professor.filter (("department", (x: StrNum) => x == "cs")).show ()

    banner ("Test map")
    def plus (v1: Int, v2: Int): Int =  v1 + v2
    professor.map ((v1: Int) => v1 + 5, "pid").show ()

    banner ("Test reduce")
    professor.reduce (plus, "pid").show ()

    banner ("Test fold")
    professor.fold (10, plus, "pid").show ()

} // RelationFTest object

