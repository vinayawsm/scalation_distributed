
//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** @author  John Miller
 *  @version 1.5
 *  @date    Sun Aug 23 15:42:06 EDT 2015
 *  @see     LICENSE (MIT style license file).
 *
 *  An implementation supporting columnar relational databases facilitating easy
 *  and rapid analytics.  The columns in a relation are vectors from the
 *  `scalation.linalgebra.mem_mapped` package.  Vectors and matrices may be readily
 *  extracted from a relation and feed into any of the numerous analytics techniques
 *  provided in `scalation.analytics`.  The implementation provides most of the
 *  columnar relational algebra operators given in the following paper:
 *  @see db.csail.mit.edu/projects/cstore/vldb.pdf
 *
 *  Some of the operators have unicode versions: @see `scalation.util.UnicodeTest`
 */

package scalation
package columnar_db

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream, PrintWriter}

import scala.collection.immutable.StringOps
import scala.collection.mutable.{ArrayBuffer, Map}
import scala.math.{min => MIN}
import scala.reflect.ClassTag

import scalation.linalgebra.MatrixKind._
import scalation.linalgebra.mem_mapped._
import scalation.math.{Complex, Rational, Real}
import scalation.math.StrO._
import scalation.util.{getFromURL_File, Error}

import TableObj._

//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `MM_Relation` companion object provides additional functions for the `MM_Relation`
 *  class.
 */
object MM_Relation
{
    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Create a relation from a sequence of row/tuples.  These rows must be converted
     *  to columns.
     *  @param name     the name of the relation
     *  @param colName  the names of columns
     *  @param row      the sequence of rows to be converted to columns for the columnar relation
     *  @param key      the column number for the primary key (< 0 => no primary key)
     *  @param domain   an optional string indicating domains for columns (e.g., 'SD' = `StrNum`, `Double`) 
     */
    def apply (name: String, colName: Seq [String], row: Seq [Row], key: Int, domain: String): MM_Relation =
    {
        val equivCol = Vector.fill [Vec] (colName.length)(null)
        val r2 = new MM_Relation (name, colName, equivCol, key, domain)
        for (tuple <- row) r2.add (tuple)
        r2
    } // apply

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Create a relation from a sequence of row/tuples.  These rows must be converted
     *  to columns.
     *  @param name     the name of the relation
     *  @param colName  the names of columns
     *  @param row      the sequence of rows to be converted to columns for the columnar relation
     *  @param key      the column number for the primary key (< 0 => no primary key)
     */
    def apply (name: String, colName: Seq [String], row: Seq [Row], key: Int): MM_Relation =
    {
        val equivCol = Vector.fill [Vec] (colName.length)(null)
        val r2 = new MM_Relation (name, colName, equivCol, key, null)
        for (tuple <- row) r2.add_2 (tuple)
        r2
    } // apply

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Read the relation with the given 'name' into memory using serialization.
     *  @param name  the name of the relation to load
     */
    def apply (name: String): MM_Relation =
    {
        val ois = new ObjectInputStream (new FileInputStream (STORE_DIR + name + SER))
        val obj = ois.readObject ()
        ois.close ()
        obj.asInstanceOf [MM_Relation]
    } // apply

    //:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Read the relation with the given 'name' into memory loading its columns
     *  with data from the CSV file named 'fileName'.
     *  @param fileName  the file name of the data file
     *  @param name      the name of the relation
     *  @param colName   the names of columns
     *  @param key       the column number for the primary key (< 0 => no primary key)
     *  @param domain    an optional string indicating domains for columns (e.g., 'SD' = `StrNum`, `Double`) 
     *  @param skip      the number of lines in the CSV file to skip (e.g., header line(s))
     *  @param eSep      the element separation string/regex (e.g., "," ";" " +")
     */
    def apply (fileName: String, name: String, colName: Seq [String], key: Int,
               domain: String, skip: Int, eSep: String): MM_Relation =
    {
        var cnt    = skip
        val lines  = getFromURL_File (fileName)
        val newCol = Vector.fill [Vec] (colName.length)(null)
        val r3     = new MM_Relation (name, colName, newCol, key, domain)
        for (ln <- lines) {
            if (cnt <= 0) r3.add (r3.row (ln.split (eSep), domain)) else cnt -= 1
        } // for
        r3
    } // apply

    //:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Read the relation with the given 'name' into memory loading its columns
     *  with data from the CSV file named 'fileName'.  In this version, the column
     *  names are read from the first line of the file.
     *  @param fileName  the file name of the data file
     *  @param name      the name of the relation
     *  @param key       the column number for the primary key (< 0 => no primary key)
     *  @param domain    an optional string indicating domains for columns (e.g., 'SD' = `StrNum`, `Double`)
     *  @param eSep      the element separation string/regex (e.g., "," ";" " +")
     */
    def apply (fileName: String, name: String, key: Int, domain: String, eSep: String): MM_Relation =
    {
        var first = true
        val lines = getFromURL_File (fileName)
        var colBuffer: Array [ArrayBuffer [String]] = null
        var colName: Seq [String] = null
        var newCol: Vector [Vec] = null

        for (ln <- lines) {
            if (first) {
                colName   = ln.split (eSep)
                colBuffer = Array.ofDim (colName.length)
                for (i <- colBuffer.indices) colBuffer(i) = new ArrayBuffer ()
                first = false
            } else {
                val values = ln.split (eSep)
                values.indices.foreach (i => { colBuffer(i) += values(i) })
            } // if
        } // for
        new MM_Relation (name, colName, colBuffer.indices.map (i => VectorS (colBuffer(i).toArray)).toVector, key, domain)
    } // apply

    //:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Read the relation with the given 'name' into memory loading its columns
     *  with data from the CSV file named 'fileName'.  This version assumes
     *  defaults for 'eSep' and 'skip' of ("," and 0).
     *  @param fileName  the file name of the data file
     *  @param name      the name of the relation
     *  @param colName   the names of columns
     *  @param key       the column number for the primary key (< 0 => no primary key)
     *  @param domain    an optional string indicating domains for columns (e.g., 'SD' = `StrNum`, `Double`) 
     */
    def apply (fileName: String, name: String, colName: Seq [String], key: Int,
               domain: String): MM_Relation =
    {
        var eSep   = ","
        val lines  = getFromURL_File (fileName)
        val newCol = Vector.fill [Vec] (colName.length)(null)
        val r3     = new MM_Relation (name, colName, newCol, key, domain)
        for (ln <- lines) r3.add (r3.row (ln.split (eSep), domain))
        r3
    } // apply

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Read the relation with the given 'name' into memory from a JSON file.
     *  @param fileName  the file name of the JSON file
     *  @param name      the name of the relation to load
     */
    def apply (fileName: String, name: String): MM_Relation =
    {
        null                                                     // FIX - needs to be implemented
    } // apply

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Create a relation from the 'xy' matrix of doubles.
     *  @param xy       the matrix containing the data
     *  @param name     the name of the relation
     *  @param colName  the names of columns
     *  @param key      the column number for the primary key (< 0 => no primary key)
     *  @param domain   an optional string indicating domains for columns (e.g., 'SD' = `StrNum`, `Double`) 
     */
    def fromMatriD (xy: MatriD, name: String, colName: Seq [String], key: Int = -1,
                    domain: String = null): MM_Relation =
    {
        val newCol = for (j <- 0 until xy.dim2) yield xy.col (j).asInstanceOf [Vec]
        new MM_Relation (name, colName, newCol.toVector, key, domain)
    } // fromMatriD

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Create a relation from the 'x' matrix of doubles and 'y' vector of doubles
     *  or integers.
     *  @param x        the matrix containing the data
     *  @param y        the vector containing the data
     *  @param name     the name of the relation
     *  @param colName  the names of columns
     *  @param key      the column number for the primary key (< 0 => no primary key)
     *  @param domain   an optional string indicating domains for columns (e.g., 'SD' = `StrNum`, `Double`) 
     */
    def fromMatriD_ (x: MatriD, y: Vec, name: String, colName: Seq [String], key: Int = -1,
                    domain: String = null): MM_Relation =
    {
        val newCol = for (j <- 0 until x.dim2) yield x.col (j).asInstanceOf [Vec]
        new MM_Relation (name, colName, newCol.toVector :+ y, key, domain)
    } // fromMatriD_

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Create a relation from the 'xy' matrix of integers.
     *  @param xy       the matrix containing the data
     *  @param name     the name of the relation
     *  @param colName  the names of columns
     *  @param key      the column number for the primary key (< 0 => no primary key)
     *  @param domain   an optional string indicating domains for columns (e.g., 'SD' = `StrNum`, `Double`) 
     */
    def fromMatriI (xy: MatriI, name: String, colName: Seq [String], key: Int = -1,
                    domain: String = null): MM_Relation =
    {
        val newCol = for (j <- 0 until xy.dim2) yield xy.col (j).asInstanceOf [Vec]
        new MM_Relation (name, colName, newCol.toVector, key, domain)
    } // fromMatriI

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Create a relation from the 'xy' matrix of integers and 'y' vector of integers.
     *  @param x        the matrix containing the data
     *  @param y        the vector containing the data
     *  @param name     the name of the relation
     *  @param colName  the names of columns
     *  @param key      the column number for the primary key (< 0 => no primary key)
     *  @param domain   an optional string indicating domains for columns (e.g., 'SD' = `StrNum`, `Double`) 
     */
    def fromMatriII (x: MatriI, y: VectorI, name: String, colName: Seq [String], key: Int = -1,
                     domain: String = null): MM_Relation =
    {
        val newCol = for (j <- 0 until x.dim2) yield x.col (j).asInstanceOf [Vec]
        new MM_Relation (name, colName, newCol.toVector :+ y, key, domain)
    } // fromMatriII

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Return the count (number of elements) of each of the columns of columnar
     *  relation 'r'.
     *  @param r  the given relation
     */
    def count (r: MM_Relation): Seq [Any] = for (j <- r.col.indices) yield r.col(j).size

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Return the minimum of each of the columns of columnar relation 'r'.
     *  @param r  the given relation
     */
    def min (r: MM_Relation): Seq [Any] = for (c <- r.col) yield Vec.min (c)

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Return the maximum of each of the columns of columnar relation 'r'.
     *  @param r  the given relation
     */
    def max (r: MM_Relation): Seq [Any] = for (c <- r.col) yield Vec.max (c)

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Compute the mean of each of the columns of columnar relation 'r'.
     *  @param r  the given relation
     */
    def sum (r: MM_Relation): Seq [Any] = for (c <- r.col) yield Vec.sum (c)

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Compute the mean of each of the columns of columnar relation 'r'.
     *  @param r  the given relation
     */
    def mean (r: MM_Relation): Seq [Any] = for (c <- r.col) yield Vec.mean (c)

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Compute the mean of each of the columns of columnar relation 'r'.
     *  @param r  the given relation
     */
    def Ɛ (r: MM_Relation): Seq [Any] = for (c <- r.col) yield Vec.mean (c)

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Compute the variance of each of the columns of columnar relation 'r'.
     *  @param r  the given relation
     */
    def variance (r: MM_Relation): Seq [Any] = for (c <- r.col) yield Vec.variance (c)

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Compute the variance of each of the columns of columnar relation 'r'.
     *  @param r  the given relation
     */
    def Ʋ (r: MM_Relation): Seq [Any] = for (c <- r.col) yield Vec.variance (c)

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Compute the correlation of column 'i' and 'j' within columnar relation 'r'.
     *  @param r  the given relation
     *  @param i  the first column vector
     *  @param j  the second column vector
     */
    def corr (r: MM_Relation, i: Int = 0, j: Int = 1): Double = Vec.corr (r.col(i), r.col(j))

} // MM_Relation object

import MM_Relation._


//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `MM_Relation` class stores and operates on vectors.  The vectors form the
 *  columns of the columnar relational datastore.  Columns may have any of the
 *  following types:
 *  <p>
 *      C - `Complex`  - `VectorC` - 128 bit complex number a + bi
 *      D - `Double`   - `VectorD` -  64 bit double precision floating point number
 *      I - `Int`      - `VectorI` -  32 bit integer
 *      L - `Long`     - `VectorL` -  64 bit long integer
 *      Q - `Rational` - `VectorQ` - 128 bit ratio of two long integers
 *      R - `Real`     - `VectorR` - 128 bit quad precision floating point number
 *      S - `StrNum`   - `VectorS` - variable length numeric string
 *      T - `TimeNum`  - `VectorT` - 96 bit time number
 *  <p>
 *------------------------------------------------------------------------------
 *  @param name     the name of the relation
 *  @param colName  the names of columns
 *  @param col      the Scala Vector of columns making up the columnar relation
 *  @param key      the column number for the primary key (< 0 => no primary key)
 *  @param domain   an optional string indicating domains for columns (e.g., 'SD' = `StrNum`, `Double`) 
 *  @param fKeys    an optional sequence of foreign keys - Seq (column name, ref table name, ref column position)
 *  @param enter    whether to enter the newly created relation into the `Catalog`
 */
class MM_Relation (val name: String, val colName: Seq [String], var col: Vector [Vec],
                   val key: Int = 0, val domain: String = null, var fKeys: Seq [(String, String, Int)] = null,
                enter: Boolean = true)
     extends Table with Error
{
    if (colName.length != col.length) flaw ("constructor", "incompatible sizes for 'colName' and 'col'")

    Catalog.add (name, colName, key, domain)

    /** The 'colMap' maps column names to column positions
     */
    private val colMap = Map [String, Int] ()
    for (j <- colName.indices) colMap += colName(j) -> j

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Return the size in terms of number of columns in the relation.
     */
    def cols: Int = col.length

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Return all of the columns in the relation.
     */
//  def columns: Vector [Vec] = col

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Return the column in the relation with column name 'cName'.
     *  @param cName  column name used to retrieve the column vector
     */
//  def column (cName: String): Vec = col(colMap (cName))

    def column(cName: String): scalation.linalgebra.Vec = ???
    def columns: Vector[scalation.linalgebra.Vec] = ???

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Return the names of columns in the relation.
     */
    def colNames: Seq [String] = colName

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Return the mapping from column names to column positions.
     */
    def colsMap: Map [String, Int] = colMap

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Return the domains for the columns in the relation.
     */
    def domains: String = domain

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Return the size in terms of number of rows in the relation.
     */
    def rows: Int = col(0).size

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Rename 'this' table, returning a shallow copy of 'this' table.
     *  @param newName  the new name for the table.
     */
    def rename (newName: String): MM_Relation =
    {
        new MM_Relation (newName, colName, col, key, domain, fKeys)
    } // rename

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Project onto the columns with the given column names.
     *  @param cName  the names of the columns to project onto
     */
    def project (cName: String*): MM_Relation = project (cName.map(colMap (_)), cName)

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Project onto the columns with the given column positions.
     *  @param cPos   the column positions to project onto
     *  @param cName  the optional new names for the columns to project onto
     */
    def project (cPos: Seq [Int], cName: Seq [String] = null): MM_Relation =
    {
        val newCName  = if (cName == null) cPos.map (colName(_)) else cName
        val newCol    = cPos.map (col(_)).toVector
        val newKey    = if (cPos contains key) key else -1
        val newDomain = projectD (domain, cPos)
        new MM_Relation (name + "_p_" + ucount (), newCName, newCol, newKey, newDomain)
    } // project

    private def getMeta (cName: String) = { val cn = colMap (cName)
                                           (cn,
                                            Seq (cName),
                                            if (cn == key) key else -1,
                                            projectD (domain, Seq (cn))) }

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Select elements from column 'cName' in 'this' relation that satisfy the
     *  predicate 'p' and project onto that column.
     *  @param cName  the name of the column used for selection
     *  @param p      the predicate (`Boolean` function) to be satisfied
     */
    def pisigmaC (cName: String, p: Complex => Boolean): MM_Relation =
    {
        val nu     = getMeta (cName)
        val newCol = Vector (col (nu._1).asInstanceOf [VectorC].filter (p))
        new MM_Relation (name + "_s_" + ucount (), nu._2, newCol, nu._3, nu._4)
    } // pisigmaC

    def pisigmaD (cName: String, p: Double => Boolean): MM_Relation =
    {
        val nu     = getMeta (cName)
        val newCol = Vector (col (nu._1).asInstanceOf [VectorD].filter (p))
        new MM_Relation (name + "_s_" + ucount (), nu._2, newCol, nu._3, nu._4)
    } // pisigmaD

    def pisigmaI (cName: String, p: Int => Boolean): MM_Relation =
    {
        val nu     = getMeta (cName)
        val newCol = Vector (col (nu._1).asInstanceOf [VectorI].filter (p))
        new MM_Relation (name + "_s_" + ucount (), nu._2, newCol, nu._3, nu._4)
    } // pisigmaI

    def pisigmaL (cName: String, p: Long => Boolean): MM_Relation =
    {
        val nu     = getMeta (cName)
        val newCol = Vector (col (nu._1).asInstanceOf [VectorL].filter (p))
        new MM_Relation (name + "_s_" + ucount (), nu._2, newCol, nu._3, nu._4)
    } // pisigmaL

    def pisigmaQ (cName: String, p: Rational => Boolean): MM_Relation =
    {
        val nu     = getMeta (cName)
        val newCol = Vector (col (nu._1).asInstanceOf [VectorQ].filter (p))
        new MM_Relation (name + "_s_" + ucount (), nu._2, newCol, nu._3, nu._4)
    } // pisigmaQ

    def pisigmaR (cName: String, p: Real => Boolean): MM_Relation =
    {
        val nu     = getMeta (cName)
        val newCol = Vector (col (nu._1).asInstanceOf [VectorR].filter (p))
        new MM_Relation (name + "_s_" + ucount (), nu._2, newCol, nu._3, nu._4)
    } // pisigmaR

    def pisigmaS (cName: String, p: StrNum => Boolean): MM_Relation =
    {
        val nu     = getMeta (cName)
        val newCol = Vector (col (nu._1).asInstanceOf [VectorS].filter (p))
        new MM_Relation (name + "_s_" + ucount (), nu._2, newCol, nu._3, nu._4)
    } // pisigmaS

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Select elements from columns in 'cName' in 'this' relation that satisfy
     *  the predicate 'p'.
     *  @param cName  the name of the column used for selection
     *  @param p      the predicate (`Boolean` function) to be satisfied
     */
    def sigma [T <: Any] (cName: String, p: T => Boolean): MM_Relation =
    {
        if (domain != null) {
            domain(colMap (cName)) match {
            case 'D' => selectAt (selectD (cName, p.asInstanceOf [Double => Boolean]))
            case 'I' => selectAt (selectI (cName, p.asInstanceOf [Int => Boolean]))
            case 'L' => selectAt (selectL (cName, p.asInstanceOf [Long => Boolean]))
            case 'S' => selectAt (selectS (cName, p.asInstanceOf [StrNum => Boolean]))
            case _  => { flaw ("sigma", "predicate type not supported"); null }
            } // match
        } else {
            flaw ("sigma", "optional domains not given - use type specific sigma?")
            null
        } // if
    } // sigma

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Select elements from columns in 'cName' in 'this' relation that satisfy
     *  the predicate 'p'.
     *  @param cName  the name of the column used for selection
     *  @param p      the predicate (`Boolean` function) to be satisfied
     */
    def σ [T <: Any] (cName: String, p: T => Boolean): MM_Relation =
    {
        if (domain != null) {
            domain(colMap (cName)) match {
            case 'D' => selectAt (selectD (cName, p.asInstanceOf [Double => Boolean]))
            case 'I' => selectAt (selectI (cName, p.asInstanceOf [Int => Boolean]))
            case 'L' => selectAt (selectL (cName, p.asInstanceOf [Long => Boolean]))
            case 'S' => selectAt (selectS (cName, p.asInstanceOf [StrNum => Boolean]))
            case _  => { flaw ("σ", "predicate type not supported"); null }
            } // match
        } else {
            flaw ("σ", "optional domains not given - use type specific sigma?")
            null
        } // if
    } // σ

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Select elements from columns in 'cName' in 'this' relation that satisfy
     *  the predicate 'p'.
     *  @param cName  the name of the column used for selection
     *  @param p      the predicate (`Boolean` function) to be satisfied
     */
    def sigmaC (cName: String, p: Complex => Boolean): MM_Relation = selectAt (selectC (cName, p))

    def sigmaD (cName: String, p: Double => Boolean): MM_Relation = selectAt (selectD (cName, p))

    def sigmaI (cName: String, p: Int => Boolean): MM_Relation = selectAt (selectI (cName, p))

    def sigmaL (cName: String, p: Long => Boolean): MM_Relation = selectAt (selectL (cName, p))

    def sigmaQ (cName: String, p: Rational => Boolean): MM_Relation = selectAt (selectQ (cName, p))

    def sigmaR (cName: String, p: Real => Boolean): MM_Relation = selectAt (selectR (cName, p))

    def sigmaS (cName: String, p: StrNum => Boolean): MM_Relation = selectAt (selectS (cName, p))

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Select the positions of elements from columns in 'cName' in 'this' relation
     *  that satisfy the predicate 'p'.
     *  @param cName  the name of the column used for selection
     *  @param p      the predicate (`Boolean` function) to be satisfied
     */
    def selectC (cName: String, p: Complex => Boolean): Seq [Int] =
    {
        col (colMap (cName)).asInstanceOf [VectorC].filterPos (p)
    } // selectC

    def selectD (cName: String, p: Double => Boolean): Seq [Int] =
    {
        col (colMap (cName)).asInstanceOf [VectorD].filterPos (p)
    } // selectD

    def selectI (cName: String, p: Int => Boolean): Seq [Int] =
    {
        col (colMap (cName)).asInstanceOf [VectorI].filterPos (p)
    } // selectI

    def selectL (cName: String, p: Long => Boolean): Seq [Int] =
    {
        col (colMap (cName)).asInstanceOf [VectorL].filterPos (p)
    } // selectL

    def selectQ (cName: String, p: Rational => Boolean): Seq [Int] =
    {
        col (colMap (cName)).asInstanceOf [VectorQ].filterPos (p)
    } // selectQ

    def selectR (cName: String, p: Real => Boolean): Seq [Int] =
    {
        col (colMap (cName)).asInstanceOf [VectorR].filterPos (p)
    } // selectR

    def selectS (cName: String, p: StrNum => Boolean): Seq [Int] =
    {
        col (colMap (cName)).asInstanceOf [VectorS].filterPos (p)
    } // selectS

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Select across all columns at the specified column positions.
     *  @param pos  the specified column positions
     */
    def selectAt (pos: Seq [Int]): MM_Relation =
    {
       val newCol = (for (j <- col.indices) yield Vec.select (col(j), pos)).toVector
       new MM_Relation (name + "_s_" + ucount (), colName, newCol, key, domain)
    } // selectAt

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Determine whether 'this' relation and 'r2' are incompatible by having
     *  differing numbers of columns or differing domain strings.
     *  @param r2  the other relation
     */
    def incompatible (_r2: Table): Boolean =
    {
         val r2 = _r2.asInstanceOf [MM_Relation]
         if (cols != r2.cols) {
             flaw ("incompatible", "differing number of columns")
             true
         } else if (domain != r2.domain) {
             flaw ("incompatible", "differing domain strings")
             true
         } else {
             false
         } // if
    } // incompatible

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Union 'this' relation and 'r2'.  Check that the two relations are compatible.
     *  @param r2  the other relation
     */
    def union (_r2: Table): MM_Relation =
    {
        val r2 = _r2.asInstanceOf [MM_Relation]
        if (incompatible (r2)) return null
        val newCol = (for (j <- col.indices) yield Vec.++ (col(j), r2.col(j)))
        new MM_Relation (name + "_u_" + ucount (), colName, newCol.toVector, -1, domain)
    } // union

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Intersect 'this' relation and 'r2'.  Check that the two relations are compatible.
     *  @param r2  the other relation
     */
    def intersect (_r2: Table): MM_Relation =
    {
        val r2 = _r2.asInstanceOf [MM_Relation]
        if (incompatible (r2)) return null
        val newCol = Vector.fill [Vec] (colName.length)(null)
        val r3 = new MM_Relation (name + "_u_" + ucount (), colName, newCol.toVector, -1, domain)
        for (i <- 0 until rows if r2 contains row(i)) r3.add (row(i))
        r3
    } // intersect

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Take the difference of 'this' relation and 'r2' ('this - r2').  Check that
     *  the two relations are compatible.
     *  @param r2  the other relation
     */
    def minus (_r2: Table): MM_Relation =
    {
        val r2 = _r2.asInstanceOf [MM_Relation]
        if (incompatible (r2)) return null
        val newCol = Vector.fill [Vec] (colName.length)(null)
        val r3 = new MM_Relation (name + "_m_" + ucount (), colName, newCol, key, domain)
        for (i <- 0 until rows if ! (r2 contains row(i))) r3.add (row(i))
        r3
    } // minus

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Join 'this' relation and 'r2 by performing an "equi-join".  Rows from both
     *  relations are compared requiring 'cName1' values to equal 'cName2' values.
     *  Disambiguate column names by appending "2" to the end of any duplicate column name.
     *  @param cName1  the join column names of this relation (e.g., the Foreign Key)
     *  @param cName2  the join column names of relation r2 (e.g., the Primary Key)
     *  @param r2      the rhs relation in the join operation
     */
    def join (cName1: Seq [String], cName2: Seq [String], _r2: Table): MM_Relation =
    {
        val r2 = _r2.asInstanceOf [MM_Relation]
        val ncols = cols + r2.cols
        val cp1   = cName1.map (colMap (_))                        // get column positions in 'this'
        val cp2   = cName2.map (r2.colMap (_))                     // get column positions in 'r2'
        if (cp1.length != cp2.length) flaw ("join", "incompatible sizes on match columns")

        val newCName  = disambiguate (colName, r2.colName)
        val newCol    = Vector.fill [Vec] (ncols) (null)
        val newKey    = key                                        // FIX
        val newDomain = domain + r2.domain
        val r3 = new MM_Relation (name + "_j_" + ucount (), newCName, newCol, newKey, newDomain)

        for (i <- 0 until rows) {
            val t = row(i)
            for (j <- 0 until r2.rows) {
                val u = r2.row(j)
                if (sameOn (t, u, cp1, cp2)) r3.add (t ++ u)
            } // for
        } // for
        r3
    } // join

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Join 'this' relation and 'r2 by performing a "natural-join".  Rows from both
     *  relations are compared requiring 'cName' values to be equal.
     *  @param cName  the common join column names for both relation
     *  @param r2     the rhs relation in the join operation
     */
    def join (cName: Seq [String], _r2: Table): MM_Relation =
    {
        val r2 = _r2.asInstanceOf [MM_Relation]
        val ncols = cols + r2.cols - cName.length
        val cp1   = cName.map (colMap (_))                         // get column positions in 'this'
        val cp2   = cName.map (r2.colMap (_))                      // get column positions in 'r2'
        val cp3   = r2.colName.map (r2.colMap (_)) diff cp2        // 'r2' specific columns

        val newCName  = uniq_union (colName, r2.colName)
        val newCol    = Vector.fill [Vec] (ncols) (null)
        val newKey    = key                                        // FIX
        val newDomain = domain + r2.domain
        val r3 = new MM_Relation (name + "_j_" + ucount (), newCName, newCol, newKey, newDomain)

        for (i <- 0 until rows) {
            val t = row(i)
            for (j <- 0 until r2.rows) {
                val u = r2.row(j)
                if (sameOn (t, u, cp1, cp2)) { val u3 = TableObj.project (u, cp3); r3.add (t ++ u3) }
            } // for
        } // for
        r3
    } // join


    /* As seen from class MM_Relation, the missing signatures are as follows.
     * For convenience, these are usable as stub implementations.
     */
    def delete [T] (p: Predicate[T]*): MM_Relation = ???
    def eproject (aggCol: AggColumn*)(cName: String*): Relation = ???
    def groupBy (cName: String*): MM_Relation = ???
    def join [T] (r2: Table, p0: Predicate2 [T], p: Predicate2 [T]*): MM_Relation = ???
    def leftJoin (cName1: String, cName2 : String, r2: Table): MM_Relation = ???
    def leftJoin (thres: Double = 0.001) (cName1: String, cName2: String, r2: Table): MM_Relation = ???
    def orderBy (_cName: String*): MM_Relation = ???
    def product (r2: Table): MM_Relation = ???
    def reverseOrderBy (_cName: String*): MM_Relation = ???
    def select [T: ClassTag] (cName: String, p: T => Boolean): MM_Relation = ???
    def update [T] (cName: String, func: T => T, pred: T => Boolean): Unit = ???
    def update [T] (cName: String, func: T => T, matchVal: T): Unit = ???
    def update [T] (cName: String, newVal: T, matchVal: T): Unit = ???


    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Determine whether 'this' relation contains a row matching the given 'tuple'.
     *  @param tuple  an aggregation of columns values (potential row)
     */
    def contains (tuple: Row): Boolean =
    {
        for (i <- 0 until rows if row(i) sameElements tuple) return true 
        false
    } // contains

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Create a row by pulling values from all columns at position 'i'.
     *  @param i  the 'i'th position
     */
    def row (i: Int): Row = col.map (Vec (_, i)).toVector

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Compress the selected columns 'cName' in 'this' table.
     *  @param cName  the names of the columns to be compressed
     */
    def compress (cName: String*)
    {
        for (c <- cName) {
            val i = colMap (c)
//          col(i).compress ()      // FIX - add compress to Vec
        } // for
    } // compress

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Uncompress the selected columns 'cName' in 'this' table.
     *  @param cName  the names of the columns to be uncompressed
     */
    def uncompress (cName: String*)
    {
        for (c <- cName) {
            val i = colMap (c)
//          col(i).uncompress ()      // FIX - add uncompress to Vec
        } // for
    } // uncompress


    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Add 'tuple to 'this' relation as a new row.
     *  FIX:  want an efficient, covariant, mutable data structure, but Array is invariant.
     *  @param tuple  an aggregation of columns values (new row)
     *  @param typ    the string of corresponding types, e.g., 'SDI'
     */
    def add (tuple: Row)
    {
        col = (for (j <- tuple.indices) yield
                   try {
                       Vec.:+ (col(j), tuple(j))
                   } catch {
                       case cce: ClassCastException =>
                            println (s"add: for column $j of tuple $tuple"); throw cce
                   } // try
              ).toVector
    } // add 

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Add 'tuple' to 'this' relation as a new row.
     *  Type is determined by sampling values for columns
     *  @param tuple  an aggregation of columns values (new row)
     */
    def add_2 (tuple: Row)
    {
        col = (for (j <- tuple.indices) yield
                   try {
                       Vec.:+ (col(j), StrNum (tuple(j).toString))
                   } catch {
                       case cce: ClassCastException =>
                           println (s"add: for column $j of tuple $tuple"); throw cce
                   } // try
              ).toVector
    } // add_2

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Convert 'this' relation into a string column by column.
     */
    override def toString: String =
    {
        var sb = new StringBuilder ("MM_Relation(" + name + ", " + key + ",\n" + colName + ",\n")
        for (i <- col.indices) sb.append (col(i) + "\n")
        sb.replace (sb.length-1, sb.length, ")").mkString
    } // toString

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Show 'this' relation row by row.
     *  @param limit  the limit on the number of rows to display
     */
    def show (limit: Int = Int.MaxValue)
    {
        val wid   = 18                                             // column width
        val rep   = wid * colName.length                           // repetition = width * # columns
        val title = s"| MM_Relation name = $name, key-column = $key "

        println (s"|-${"-"*rep}-|")
        println (title + " "*(rep-title.length) + "   |")
        println (s"|-${"-"*rep}-|")
        print ("| "); for (cn <- colName) print (s"%${wid}s".format (cn)); println (" |")
        println (s"|-${"-"*rep}-|")
        for (i <- 0 until MIN (rows, limit)) {
            print ("| ")
            for (cv <- row(i)) {
                if (cv.isInstanceOf [Double]) print (s"%${wid}g".format (cv))
                else                          print (s"%${wid}s".format (cv))
            } // for
            println (" |")
        } // for
        println (s"|-${"-"*rep}-|")
    } // show

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Convert 'this' relation into a matrix of doubles, e.g., 
     *  <p>
     *       in the regression equation: 'xb = y' create matrix 'xy'
     *  <p>
     *  @param colPos  the column positions to use for the matrix
     *  @param kind    the kind of matrix to create
     */
    def toMatriD (colPos: Seq [Int], kind: MatrixKind = DENSE): MatriD =
    {
        val colVec = for (x <- project (colPos).col) yield Vec.toDouble (x)
        kind match {
        case DENSE           => MatrixD (colVec)
//      case SPARSE          => SparseMatrixD (colVec)
//      case SYM_TRIDIAGONAL => SymTriMatrixD (colVec)
//      case BIDIAGONAL      => BidMatrixD (colVec)
        } // match
    } // toMatriD

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Convert 'this' relation into a matrix of doubles and a vector of doubles.
     *  <p>
     *       in the regression equation: 'xb = y' create matrix 'x' and vector 'y'
     *  <p>
     *  @param colPos   the column positions to use for the matrix
     *  @param colPosV  the column position to use for the vector
     *  @param kind     the kind of matrix to create
     */
    def toMatriDD (colPos: Seq [Int], colPosV: Int, kind: MatrixKind = DENSE): Tuple2 [MatriD, VectorD] =
    {
        val colVec = for (x <- project (colPos).col) yield Vec.toDouble (x)
        kind match {
        case DENSE           => (MatrixD (colVec), Vec.toDouble (col(colPosV)))
//      case SPARSE          => (SparseMatrixD (colVec), Vec.toDouble (col(colPosV)))
//      case SYM_TRIDIAGONAL => (SymTriMatrixD (colVec), Vec.toDouble (col(colPosV)))
//      case BIDIAGONAL      => (BidMatrixD (colVec), Vec.toDouble (col(colPosV)))
        } // match
    } // toMatriDD

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Convert 'this' relation into a matrix of doubles and a vector of integers.
     *  <p>
     *       in the regression equation: 'xb = y' create matrix 'x' and vector 'y'
     *  <p>
     *  @param colPos   the column positions to use for the matrix
     *  @param colPosV  the column position to use for the vector
     *  @param kind     the kind of matrix to create
     */
    def toMatriDI (colPos: Seq [Int], colPosV: Int, kind: MatrixKind = DENSE): Tuple2 [MatriD, VectorI] =
    {
        val colVec = for (x <- project (colPos).col) yield Vec.toDouble (x)
        kind match {
        case DENSE           => (MatrixD (colVec), Vec.toInt (col(colPosV)))
//      case SPARSE          => (SparseMatrixD (colVec), Vec.toInt (col(colPosV)))
//      case SYM_TRIDIAGONAL => (SymTriMatrixD (colVec), Vec.toInt (col(colPosV)))
//      case BIDIAGONAL      => (BidMatrixD (colVec), Vec.toInt (col(colPosV)))
        } // match
    } // toMatriDI

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Convert 'this' relation into a matrix of integers.
     *  <p>
     *       in the regression equation: 'xb = y' create matrix 'xy'
     *  <p>
     *  @param colPos  the column positions to use for the matrix
     *  @param kind    the kind of matrix to create
     */
    def toMatriI (colPos: Seq [Int], kind: MatrixKind = DENSE): MatriI =
    {
        val colVec = for (x <- project (colPos).col) yield Vec.toInt (x)
        kind match {
        case DENSE           => MatrixI (colVec)
//      case SPARSE          => SparseMatrixI (colVec)
//      case SYM_TRIDIAGONAL => SymTriMatrixI (colVec)
//      case BIDIAGONAL      => BidMatrixI (colVec)
        } // match
    } // toMatriI

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Convert 'this' relation into a matrix of integers and a vector of integers.
     *  <p>
     *       in the regression equation: 'xb = y' create matrix 'x' and vector 'y'
     *  <p>
     *  @param colPos   the column positions to use for the matrix
     *  @param colPosV  the column position to use for the vector
     *  @param kind     the kind of matrix to create
     */
    def toMatriII (colPos: Seq [Int], colPosV: Int, kind: MatrixKind = DENSE): Tuple2 [MatriI, VectorI] =
    {
        val colVec = for (x <- project (colPos).col) yield Vec.toInt (x)
        kind match {
        case DENSE           => (MatrixI (colVec), Vec.toInt (col(colPosV)))
//      case SPARSE          => (SparseMatrixI (colVec), Vec.toInt (col(colPosV)))
//      case SYM_TRIDIAGONAL => (SymTriMatrixI (colVec), Vec.toInt (col(colPosV)))
//      case BIDIAGONAL      => (BidMatrixI (colVec), Vec.toInt (col(colPosV)))
        } // match
    } // toMatriII

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Save 'this' relation in a file using serialization.
     */
    def save ()
    {
        val oos = new ObjectOutputStream (new FileOutputStream (STORE_DIR + name + SER))
        oos.writeObject (this)
        oos.close ()
    } // save

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Write 'this' relation into a CSV file with each row written to a line.
     *  @param fileName  the file name of the data file
     */
    def writeCSV (fileName: String)
    {
        val out = new PrintWriter (DATA_DIR + fileName)
        for (i <- 0 until rows) out.println (row(i).toString.drop (7).dropRight (1))
        out.close
    } // writeCSV

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Write 'this' relation into a JSON file.
     *  @param fileName  the file name of the data file
     */
    def writeJSON (fileName: String)
    {
        // FIX - to be implemented
    } // writeJSON
  
} // MM_Relation class


//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `MM_RelationEx` object provides and example relation for testing.
 *  @see www.codeproject.com/Articles/652108/Create-First-Data-WareHouse
 */
object MM_RelationEx
{
    val productSales = MM_Relation ("productSales",
        Seq ("SalesInvoiceNumber", "SalesDateKey", "SalesTimeKey", "SalesTimeAltKey", "StoreID", "CustomerID",
             "ProductID", "SalesPersonID", "Quantity", "ProductActualCost", "SalesTotalCost", "Deviation"),
        Seq (Vector [Any] (1,  20130101, 44347, 121907, 1, 1, 1, 1, 2,  11.0,  13.0, 2.0),
             Vector [Any] (1,  20130101, 44347, 121907, 1, 1, 2, 1, 1,  22.5,  24.0, 1.5),
             Vector [Any] (1,  20130101, 44347, 121907, 1, 1, 3, 1, 1,  42.0,  43.5, 1.5),
             Vector [Any] (2,  20130101, 44519, 122159, 1, 2, 3, 1, 1,  42.0,  43.5, 1.5),
             Vector [Any] (2,  20130101, 44519, 122159, 1, 2, 4, 1, 3,  54.0,  60.0, 6.0),
             Vector [Any] (3,  20130101, 52415, 143335, 1, 3, 2, 2, 2,  11.0,  13.0, 2.0),
             Vector [Any] (3,  20130101, 52415, 143335, 1, 3, 3, 2, 1,  42.0,  43.5, 1.5),
             Vector [Any] (3,  20130101, 52415, 143335, 1, 3, 4, 2, 3,  54.0,  60.0, 6.0),
             Vector [Any] (3,  20130101, 52415, 143335, 1, 3, 5, 2, 1, 135.0, 139.0, 4.0),
             Vector [Any] (4,  20130102, 44347, 121907, 1, 1, 1, 1, 2,  11.0,  13.0, 2.0),
             Vector [Any] (4,  20130102, 44347, 121907, 1, 1, 2, 1, 1,  22.5,  24.0, 1.5),
             Vector [Any] (5,  20130102, 44519, 122159, 1, 2, 3, 1, 1,  42.0,  43.5, 1.5),
             Vector [Any] (5,  20130102, 44519, 122159, 1, 2, 4, 1, 3,  54.0,  60.0, 6.0),
             Vector [Any] (6,  20130102, 52415, 143335, 1, 3, 2, 2, 2,  11.0,  13.0, 2.0),
             Vector [Any] (6,  20130102, 52415, 143335, 1, 3, 5, 2, 1, 135.0, 139.0, 4.0),
             Vector [Any] (7,  20130102, 44347, 121907, 2, 1, 4, 3, 3,  54.0,  60.0, 6.0),
             Vector [Any] (7,  20130102, 44347, 121907, 2, 1, 5, 3, 1, 135.0, 139.0, 4.0),
             Vector [Any] (8,  20130103, 59326, 162846, 1, 1, 3, 1, 2,  84.0,  87.0, 3.0),
             Vector [Any] (8,  20130103, 59326, 162846, 1, 1, 4, 1, 3,  54.0,  60.0, 3.0),
             Vector [Any] (9,  20130103, 59349, 162909, 1, 2, 1, 1, 1,   5.5,   6.5, 1.0),
             Vector [Any] (9,  20130103, 59349, 162909, 1, 2, 2, 1, 1,  22.5,  24.0, 1.5),
             Vector [Any] (10, 20130103, 67390, 184310, 1, 3, 1, 2, 2,  11.0,  13.0, 2.0),
             Vector [Any] (10, 20130103, 67390, 184310, 1, 3, 4, 2, 3,  54.0,  60.0, 6.0),
             Vector [Any] (11, 20130103, 74877, 204757, 2, 1, 2, 3, 1,   5.5,   6.5, 1.0),
             Vector [Any] (11, 20130103, 74877, 204757, 2, 1, 3, 3, 1,  42.0,  43.5, 1.5)),
             0, "IIIIIIIIIDDD")

} // MM_RelationEx object


//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `MM_RelationTest` object tests the operations provided by `MM_Relation`.
 *  > runMain scalation.columnar_db.MM_RelationTest
 */
object MM_RelationTest extends App
{
    val weekdays = new MM_Relation ("weekdays", Seq ("day", "time"),
                              Vector (VectorS ("Mon", "Tue", "Wed", "Thu", "Fri"),
                                      VectorD (5.00, 8.15, 6.30, 9.45, 7.00)),
                              0, "SD")

    val weekend = new MM_Relation ("weekends", Seq ("day", "time"),
                              Vector (VectorS ("Sat", "Sun"),
                                      VectorD (3.00, 4.30)),
                              0, "SD")

    println ("--------------------------------------------")
    println ("weekdays                                  = " + weekdays)
    println ("--------------------------------------------")
    println ("weekend                                   = " + weekend)
    println ("--------------------------------------------")
    println ("weekdays.pi (\"day\")                     = " + weekdays.pi ("day"))
    println ("--------------------------------------------")
    println ("weekdays.pisigmaS (\"day\", _ == \"Mon\") = " + weekdays.pisigmaS ("day", _ == "Mon"))
    println ("--------------------------------------------")
    println ("weekdays.sigmaS (\"day\", _ == \"Mon\")   = " + weekdays.sigmaS ("day", _ == "Mon"))
    println ("--------------------------------------------")
    println ("weekdays.sigma (\"day\", _ == \"Mon\")    = " + weekdays.sigma ("day", (x: StrNum) => x == "Mon"))
    println ("--------------------------------------------")
    println ("weekdays.sigma (\"time\", _ == 5.00)      = " + weekdays.sigma ("time", (x: Double) => x == 5.00))
    println ("--------------------------------------------")
    println ("weekdays.sigmaS (\"day\", _ > \"Mon\")    = " + weekdays.sigmaS ("day", _ > "Mon"))
    println ("--------------------------------------------")
    println ("weekdays.selectS (\"day\", _ > \"Mon\")   = " + weekdays.selectS ("day", _ > "Mon"))
    println ("--------------------------------------------")
    println ("weekdays.sigmaSD (\"day\", \"time\")      = " + weekdays.sigmaS ("day",  _ == "Mon")
                                                                      .sigmaD ("time", _ == 5.00))

    val week = weekdays.union (weekend)
    println ("--------------------------------------------")
    println ("weekdays.union (weekend)                  = " + week)
    println ("--------------------------------------------")
    
    weekend.add (Vector ("Zday", 1.00))
    println ("weekend add (\"Zday\", 1.00))             = " + weekend)
    println ("--------------------------------------------")
    println ("week - weekend                            = " + (week - weekend))
    println ("--------------------------------------------")
    println ("week.join (\"day\", \"day\" weekend)      = " + week.join ("day", "day", weekend))
    println ("--------------------------------------------")
    println ("week join weekend                           = " + (week join weekend))

    week.writeCSV ("columnar_db" + ⁄ + "week.csv")

} // MM_RelationTest object


//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `MM_RelationTest2` object tests the operations provided by `MM_Relation`.
 *  The relational algebra operators are given using Unicode.
 *  @see en.wikipedia.org/wiki/List_of_Unicode_characters
 *  > runMain scalation.columnar_db.MM_RelationTest2
 */
object MM_RelationTest2 extends App
{
    val weekdays = new MM_Relation ("weekdays", Seq ("day", "time"),
                              Vector (VectorS ("Mon", "Tue", "Wed", "Thu", "Fri"),
                                      VectorD (5.00, 8.15, 6.30, 9.45, 7.00)),
                              0, "SD")

    val weekend = new MM_Relation ("weekends", Seq ("day", "time"),
                              Vector (VectorS ("Sat", "Sun"),
                                      VectorD (3.00, 4.30)),
                              0, "SD")

    println ("weekdays.π (\"day\")               = " + weekdays.π ("day"))
    println ("-------------------------------------")
    println ("weekdays.π (\"time\")              = " + weekdays.π ("time"))
    println ("-------------------------------------")
    println ("weekdays.σ (\"day\", _ == \"Mon\") = " + weekdays.σ ("day", (x: StrNum) => x == "Mon"))
    println ("-------------------------------------")
    println ("weekdays.σ (\"time\", _ == 5.00)   = " + weekdays.σ ("time", (x: Double) => x == 5.00))
    println ("-------------------------------------")
    println ("weekdays.σ (\"day\", _ > \"Mon\")  = " + weekdays.σ ("day", (x: StrNum) => x > "Mon"))
    println ("-------------------------------------")
    println ("weekdays.σ (\"time\", _ > 5.00)    = " + weekdays.σ ("time", (x: Double) => x > 5.00))
    println ("-------------------------------------")
    println ("weekdays.σ (\"day\", \"time\")     = " + weekdays.σ ("day",  (x: StrNum) => x == "Mon")
                                                               .σ ("time", (x: Double) => x == 5.00))
    val week = weekdays ⋃ weekend
    println ("-------------------------------------")
    println ("weekdays ⋃ weekend)                = " + weekdays ⋃ weekend)
    println ("-------------------------------------")
    println ("week ⋂ weekend                     = " + (week ⋂ weekend))
    println ("-------------------------------------")
    println ("week - weekend                     = " + (week - weekend))
    println ("-------------------------------------")
    println ("week ⋈ weekend                     = " + (week ⋈ weekend))

} // MM_RelationTest2 object


//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `MM_RelationTest3` object tests the operations provided by `MM_Relation`.
 *  It test various aggregate/OLAP operations on a simple data warehouse fact table.
 *  @see www.codeproject.com/Articles/652108/Create-First-Data-WareHouse
 *  FIX - allow entering doubles as "13" rather than "13.0"
 *  > runMain scalation.columnar_db.MM_RelationTest3
 */
object MM_RelationTest3 extends App
{
    import MM_Relation.{max, min}
    import MM_RelationEx.productSales

/*
    val productSales = new MM_Relation ("productSales",
        Seq ("SalesInvoiceNumber", "SalesDateKey", "SalesTimeKey", "SalesTimeAltKey", "StoreID", "CustomerID",
             "ProductID", "SalesPersonID", "Quantity", "ProductActualCost", "SalesTotalCost", "Deviation"),
        Seq (Vector [Any] (1,  20130101, 44347, 121907, 1, 1, 1, 1, 2,  11.0,  13.0, 2.0), 
             Vector [Any] (1,  20130101, 44347, 121907, 1, 1, 2, 1, 1,  22.5,  24.0, 1.5), 
             Vector [Any] (1,  20130101, 44347, 121907, 1, 1, 3, 1, 1,  42.0,  43.5, 1.5), 
             Vector [Any] (2,  20130101, 44519, 122159, 1, 2, 3, 1, 1,  42.0,  43.5, 1.5), 
             Vector [Any] (2,  20130101, 44519, 122159, 1, 2, 4, 1, 3,  54.0,  60.0, 6.0), 
             Vector [Any] (3,  20130101, 52415, 143335, 1, 3, 2, 2, 2,  11.0,  13.0, 2.0), 
             Vector [Any] (3,  20130101, 52415, 143335, 1, 3, 3, 2, 1,  42.0,  43.5, 1.5), 
             Vector [Any] (3,  20130101, 52415, 143335, 1, 3, 4, 2, 3,  54.0,  60.0, 6.0), 
             Vector [Any] (3,  20130101, 52415, 143335, 1, 3, 5, 2, 1, 135.0, 139.0, 4.0), 
             Vector [Any] (4,  20130102, 44347, 121907, 1, 1, 1, 1, 2,  11.0,  13.0, 2.0), 
             Vector [Any] (4,  20130102, 44347, 121907, 1, 1, 2, 1, 1,  22.5,  24.0, 1.5), 
             Vector [Any] (5,  20130102, 44519, 122159, 1, 2, 3, 1, 1,  42.0,  43.5, 1.5), 
             Vector [Any] (5,  20130102, 44519, 122159, 1, 2, 4, 1, 3,  54.0,  60.0, 6.0), 
             Vector [Any] (6,  20130102, 52415, 143335, 1, 3, 2, 2, 2,  11.0,  13.0, 2.0), 
             Vector [Any] (6,  20130102, 52415, 143335, 1, 3, 5, 2, 1, 135.0, 139.0, 4.0), 
             Vector [Any] (7,  20130102, 44347, 121907, 2, 1, 4, 3, 3,  54.0,  60.0, 6.0), 
             Vector [Any] (7,  20130102, 44347, 121907, 2, 1, 5, 3, 1, 135.0, 139.0, 4.0), 
             Vector [Any] (8,  20130103, 59326, 162846, 1, 1, 3, 1, 2,  84.0,  87.0, 3.0), 
             Vector [Any] (8,  20130103, 59326, 162846, 1, 1, 4, 1, 3,  54.0,  60.0, 3.0), 
             Vector [Any] (9,  20130103, 59349, 162909, 1, 2, 1, 1, 1,   5.5,   6.5, 1.0), 
             Vector [Any] (9,  20130103, 59349, 162909, 1, 2, 2, 1, 1,  22.5,  24.0, 1.5), 
             Vector [Any] (10, 20130103, 67390, 184310, 1, 3, 1, 2, 2,  11.0,  13.0, 2.0), 
             Vector [Any] (10, 20130103, 67390, 184310, 1, 3, 4, 2, 3,  54.0,  60.0, 6.0), 
             Vector [Any] (11, 20130103, 74877, 204757, 2, 1, 2, 3, 1,   5.5,   6.5, 1.0), 
             Vector [Any] (11, 20130103, 74877, 204757, 2, 1, 3, 3, 1,  42.0,  43.5, 1.5)),
             0, "IIIIIIIIIDDD")
*/

    val costVprice = productSales.project ("ProductActualCost", "SalesTotalCost")

    productSales.show ()

    println ("productSales = " + productSales)
    println ("productSales.project (\"ProductActualCost\", \"SalesTotalCost\") = " + costVprice)

    println ("\nTest count")
    println ("------------------------")
    println ("count (productSales)  = " + count (productSales))
    println ("------------------------")
    println ("count (costVprice)    = " + count (costVprice))

    println ("\nTest min")
    println ("------------------------")
    println ("min (productSales)    = " + min (productSales))
    println ("------------------------")
    println ("min (costVprice)      = " + min (costVprice))

    println ("\nTest max")
    println ("------------------------")
    println ("max (productSales)    = " + max (productSales))
    println ("------------------------")
    println ("max (costVprice)      = " + max (costVprice))

    println ("\nTest sum")
    println ("------------------------")
    println ("sum (productSales)    = " + sum (productSales))
    println ("------------------------")
    println ("sum (costVprice)      = " + sum (costVprice))

    println ("\nTest expectation/mean")
    println ("------------------------")
    println ("Ɛ (productSales)      = " + Ɛ (productSales))
    println ("------------------------")
    println ("Ɛ (costVprice)        = " + Ɛ (costVprice))

    println ("\nTest variance")
    println ("------------------------")
    println ("Ʋ (productSales)      = " + Ʋ (productSales))
    println ("------------------------")
    println ("Ʋ (costVprice)        = " + Ʋ (costVprice))

    println ("\nTest correlation")
    println ("------------------------")
    println ("corr (costVprice)     = " + corr (costVprice))

} // MM_RelationTest3 object


//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `MM_RelationTest4` object tests conversion `MM_Relation` to a matrix.
 *  > runMain scalation.columnar_db.MM_RelationTest4
 */
object MM_RelationTest4 extends App
{
    import MM_RelationEx.productSales

    val (mat, vec) = productSales.toMatriDD (0 to 10, 11)

    println ("mat = " + mat)
    println ("vec = " + vec)

} // MM_RelationTest4 object

