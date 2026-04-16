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
package org.squeryl.dsl

/**
 * Represents the grouping key portion of a SQL `GROUP BY` result. Wraps the key value
 * produced by the `groupBy` clause in a Squeryl query.
 *
 * @tparam K the type of the grouping key (can be a single value or a tuple for composite keys)
 */
class Group[K](k: K) {
  /** Returns the grouping key value. */
  def key = k
}

/**
 * Wraps the aggregate measure values produced by a Squeryl `compute` clause. Used
 * for queries that produce aggregate results without a `groupBy` (i.e., aggregating
 * over the entire result set).
 *
 * Also used as a wrapper type for scalar subqueries in comparison operations
 * (e.g., `field === subquery` where the subquery returns a single `Measures` value).
 *
 * @tparam M the type of the measure value (can be a single value or a tuple for multiple aggregates)
 */
class Measures[M](m: M) {
  /** Returns the aggregate measure value(s). */
  def measures = m
}

/**
 * Combines a grouping key with aggregate measure values, representing a single row
 * in the result of a SQL `GROUP BY` query with aggregate functions. This is the
 * return type of Squeryl's `groupBy(...).compute(...)` queries.
 *
 * @tparam K the type of the grouping key
 * @tparam M the type of the aggregate measure(s)
 */
class GroupWithMeasures[K, M](k: K, m: M) {
  def key = k
  def measures = m

  override def toString = {
    val sb = new java.lang.StringBuilder
    sb.append("GroupWithMeasures[")
    sb.append("key=")
    sb.append(key)
    sb.append(",measures=")
    sb.append(measures)
    sb.append("]")
    sb.toString
  }
}

/** Companion object providing an extractor for pattern matching on [[GroupWithMeasures]] instances. */
object GroupWithMeasures {
  /** Extracts the key and measures from a [[GroupWithMeasures]] for use in pattern matching. */
  def unapply[K, M](x: GroupWithMeasures[K, M]) = Some((x.key, x.measures))
}
