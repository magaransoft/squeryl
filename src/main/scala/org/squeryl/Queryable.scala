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

import dsl.ast.LogicalBoolean
import dsl.QueryDsl
import internals.ResultSetMapper
import java.sql.ResultSet

/**
 * Base trait for all queryable sources in Squeryl, including [[Table]]s, [[View]]s, and [[Query]] results.
 *
 * `Queryable` provides the foundation for constructing type-safe SQL queries. It exposes a
 * [[where]] method for filtering rows and an internal `give` method used by the framework to
 * materialize row objects from JDBC result sets.
 *
 * @tparam T the row type produced by this queryable source
 */
trait Queryable[T] {

  /** The name of this queryable source (table name, view name, or synthetic query name). */
  def name: String

  private[squeryl] var inhibited = false

  private[squeryl] def give(resultSetMapper: ResultSetMapper, rs: ResultSet): T

  /**
   * Creates a query that selects all rows from this source matching the given WHERE clause.
   *
   * @param whereClauseFunctor a function that takes a sample instance and returns a logical condition
   * @param dsl the implicit QueryDsl
   * @return a [[Query]] filtered by the given condition
   */
  def where(whereClauseFunctor: T => LogicalBoolean)(implicit dsl: QueryDsl): Query[T] = {
    import dsl._
    from(this)(q0 => dsl.where(whereClauseFunctor(q0)).select(q0))
  }
}
