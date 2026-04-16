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
 ******************************************************************************/
package org.squeryl.logging

/**
 * Captures metadata about a single SQL statement execution, including timing, row count,
 * the JDBC statement text, and a unique identifier for correlation with result set events.
 */
class StatementInvocationEvent(
  _definitionOrCallSite: StackTraceElement,
  val start: Long,
  val end: Long,
  val rowCount: Int,
  val jdbcStatement: String
) {

  /** A unique identifier for this invocation event, used to correlate with result set iteration events. */
  val uuid = {
    val tmp = java.util.UUID.randomUUID
    java.lang.Long.toHexString(tmp.getMostSignificantBits) + "-" +
      java.lang.Long.toHexString(tmp.getLeastSignificantBits)
  }

  /** Returns the string representation of the stack trace element identifying where this statement was defined or called. */
  def definitionOrCallSite =
    _definitionOrCallSite.toString
}

/**
 * Listener interface for receiving notifications about SQL statement executions.
 * Implementations can collect performance metrics, log queries, or feed profiling tools.
 */
trait StatisticsListener {

  /** Called after a SELECT query is executed. */
  def queryExecuted(se: StatementInvocationEvent): Unit

  /** Called after result set iteration completes or is abandoned. */
  def resultSetIterationEnded(
    statementInvocationId: String,
    iterationEndTime: Long,
    rowCount: Int,
    iterationCompleted: Boolean
  ): Unit

  /** Called after an UPDATE statement is executed. */
  def updateExecuted(se: StatementInvocationEvent): Unit

  /** Called after an INSERT statement is executed. */
  def insertExecuted(se: StatementInvocationEvent): Unit

  /** Called after a DELETE statement is executed. */
  def deleteExecuted(se: StatementInvocationEvent): Unit
}

/** Utility for marking stack frames as the last Squeryl-internal frame, used by profiling tools to identify the call site boundary between user code and Squeryl internals. */
object StackMarker {

  /** Executes the given block, marking this stack frame as the last Squeryl-internal frame for profiling purposes. */
  def lastSquerylStackFrame[A](a: => A) = a
}
