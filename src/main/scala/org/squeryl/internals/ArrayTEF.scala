package org.squeryl.internals

import org.squeryl.Session
import org.squeryl.dsl.{ArrayJdbcMapper, TypedExpressionFactory}

import java.sql
import java.sql.ResultSet

/**
 * Abstract base class for typed expression factories that handle SQL array columns.
 * Maps between Scala `Array[P]` and JDBC `java.sql.Array`, using the current session's
 * connection to create JDBC array instances and the database adapter to determine the
 * appropriate SQL array element type name.
 *
 * Subclasses must provide:
 *  - `sample`: a non-empty sample array (e.g., `Array[Int](0)`) for type introspection
 *  - `toWrappedJDBCType`: converts a Scala array element to its JDBC-compatible boxed form
 *  - `fromWrappedJDBCType`: converts a JDBC `Object[]` back to a Scala `Array[P]`
 *
 * @tparam P  the Scala element type of the array (e.g., `Int`, `Long`, `String`)
 * @tparam TE the type tag for the expression system (e.g., [[org.squeryl.dsl.TIntArray]], [[org.squeryl.dsl.TStringArray]])
 */
abstract class ArrayTEF[P, TE]
    extends TypedExpressionFactory[Array[P], TE]
    with ArrayJdbcMapper[java.sql.Array, Array[P]] {
  // must define "sample" that includes an element. e.g. Array[Int](0)
  def sample: Array[P]

  /**
   * Converts a single Scala array element to its JDBC-compatible boxed `Object` form.
   * For example, converts a Scala `Int` to a `java.lang.Integer`.
   */
  def toWrappedJDBCType(element: P): java.lang.Object

  /**
   * Converts an array of JDBC boxed objects back to a Scala `Array[P]`.
   * Called when reading array values from the database.
   */
  def fromWrappedJDBCType(element: Array[java.lang.Object]): Array[P]

  val defaultColumnLength = 1

  def extractNativeJdbcValue(rs: ResultSet, i: Int): sql.Array = rs.getArray(i)

  /**
   * Converts a Scala `Array[P]` to a `java.sql.Array` by mapping each element through
   * `toWrappedJDBCType` and using the current session's connection to create the JDBC array.
   * The SQL type name is determined by the database adapter's `arrayCreationType` method.
   */
  def convertToJdbc(v: Array[P]): java.sql.Array = {
    val content: Array[java.lang.Object] = v.map(toWrappedJDBCType)
    val s = Session.currentSession
    val con = s.connection
    var rv: java.sql.Array = null
    try {
      val typ = s.databaseAdapter.arrayCreationType(toWrappedJDBCType(sample(0)).getClass)
      rv = con.createArrayOf(typ, content)
    } catch {
      case e: Exception => s.log("Cannot create JDBC array: " + e.getMessage)
    }
    rv
  }

  /**
   * Converts a `java.sql.Array` from the database back to a Scala `Array[P]` by
   * extracting the underlying `Object[]` and passing it through `fromWrappedJDBCType`.
   */
  def convertFromJdbc(v: java.sql.Array): Array[P] = {
    val s = Session.currentSession
    var rv: Array[P] = sample.take(0)
    try {
      val obj = v.getArray();
      rv = fromWrappedJDBCType(obj.asInstanceOf[Array[java.lang.Object]])
    } catch {
      case e: Exception => s.log("Cannot obtain array from JDBC: " + e.getMessage)
    }
    rv
  }

}
