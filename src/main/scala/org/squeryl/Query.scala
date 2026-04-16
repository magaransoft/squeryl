/*******************************************************************************
 * Copyright 2010 Maxime Lévesque
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ***************************************************************************** */
package org.squeryl

import dsl.ast._
import internals.ResultSetMapper
import java.sql.ResultSet

/**
 * Represents a compiled SQL query that yields results of type `R`.
 *
 * `Query` extends [[Queryable]], meaning query results can themselves be used as sources
 * in further queries (subqueries). It provides methods to execute the query and retrieve
 * results ([[iterator]], [[single]], [[singleOption]], [[headOption]]), inspect the generated
 * SQL ([[statement]], [[dumpAst]]), and compose with other queries via set operations
 * ([[union]], [[intersect]], [[except]] and their `All` variants).
 *
 * Queries are typically constructed using the DSL methods in [[dsl.QueryDsl]] (e.g.
 * `from`, `where`, `select`, `groupBy`, `orderBy`) rather than instantiated directly.
 *
 * @tparam R the result row type
 */
trait Query[R] extends Queryable[R] {

  /**
   * Executes this query and returns an iterator over the result rows.
   *
   * The underlying JDBC `ResultSet` and `Statement` are managed by the iterator; the statement
   * is closed when the iterator is exhausted or when the enclosing session/transaction ends.
   *
   * @return an iterator over the query results
   */
  def iterator: Iterator[R]

  protected[squeryl] def invokeYield(rsm: ResultSetMapper, resultSet: ResultSet): R

  /**
   * Returns a debug string representation of this query's abstract syntax tree.
   *
   * Useful for troubleshooting query construction and understanding the internal
   * expression tree structure.
   *
   * @return a string dump of the AST
   */
  def dumpAst: String

  /**
   * Returns the SQL statement with parameter values inlined (not as '?' placeholders).
   *
   * This produces a human-readable "pretty" SQL string useful for debugging and logging.
   * Note: this is not necessarily the exact SQL sent to the database (which uses prepared
   * statement parameters).
   *
   * @return the SQL statement string with values substituted
   */
  def statement: String

  /**
   * Returns the root expression node of this query's abstract syntax tree.
   *
   * @return the AST root node
   */
  def ast: ExpressionNode

  private[squeryl] def copy(asRoot: Boolean, newUnions: List[(String, Query[R])]): Query[R]

  /**
   * Returns the first row of the query. An exception will be thrown
   * if the query returns no row or more than one row.
   */
  def single: R = {
    val i = iterator
    val r = i.next()
    if (i.hasNext)
      org.squeryl.internals.Utils.throwError("single called on query returning more than one row : \n" + statement)
    r
  }

  /**
   * Returns Some(singleRow), None if there are none, throws an exception 
   * if the query returns more than one row.
   */
  def singleOption: Option[R] = {
    val i = iterator
    val res =
      if (i.hasNext)
        Some(i.next())
      else
        None

    if (i.hasNext)
      org.squeryl.internals.Utils
        .throwError("singleOption called on query returning more than one row : \n" + statement)
    res
  }

  /**
   * Returns the first row of the query result, or `None` if the query returns no rows.
   *
   * Unlike [[singleOption]], this does not throw an exception if more than one row exists.
   *
   * @return `Some(firstRow)` or `None`
   */
  def headOption = {
    val i = iterator
    if (i.hasNext)
      Some(i.next())
    else
      None
  }

  /** Combines this query's results with another query using SQL UNION (removes duplicates). */
  def union(q: Query[R]): Query[R]

  /** Combines this query's results with another query using SQL UNION ALL (keeps duplicates). */
  def unionAll(q: Query[R]): Query[R]

  /** Returns only rows present in both this query and the given query (SQL INTERSECT). */
  def intersect(q: Query[R]): Query[R]

  /** Returns rows present in both queries, keeping duplicates (SQL INTERSECT ALL). */
  def intersectAll(q: Query[R]): Query[R]

  /** Returns rows in this query that are not in the given query (SQL EXCEPT). */
  def except(q: Query[R]): Query[R]

  /** Returns rows in this query not in the given query, keeping duplicates (SQL EXCEPT ALL). */
  def exceptAll(q: Query[R]): Query[R]

  /** Returns a new query that applies SQL DISTINCT to eliminate duplicate result rows. */
  def distinct: Query[R]

  /** Returns a new query with a FOR UPDATE lock hint, causing selected rows to be locked. */
  def forUpdate: Query[R]

  /**
   * Returns a new query with pagination applied (SQL LIMIT/OFFSET or equivalent).
   *
   * @param offset the number of rows to skip
   * @param pageLength the maximum number of rows to return
   */
  def page(offset: Int, pageLength: Int): Query[R]

  private[squeryl] def root: Option[Query[R]] = None
}
