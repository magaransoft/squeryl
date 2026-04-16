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

import logging.StatisticsListener
import org.squeryl.internals._
import collection.mutable.ArrayBuffer
import java.sql.{SQLException, ResultSet, Statement, Connection}
import scala.util.control.ControlThrowable

/**
 * A session implementation that defers obtaining a JDBC connection until the first
 * database operation is performed. This is useful when a session is created speculatively
 * (e.g., at the start of a request) but may not actually need database access.
 *
 * The connection is obtained lazily from `connectionFunc` on first access to `connection`.
 * Once obtained, auto-commit is disabled and the original transaction isolation level is
 * saved for restoration after the transaction completes.
 *
 * @param connectionFunc     a function that provides a JDBC connection on demand
 * @param databaseAdapter    the database adapter for SQL dialect-specific operations
 * @param statisticsListener optional listener for query statistics
 */
class LazySession(
  val connectionFunc: () => Connection,
  val databaseAdapter: DatabaseAdapter,
  val statisticsListener: Option[StatisticsListener] = None
) extends AbstractSession {

  private[this] var _connection: Option[Connection] = None

  /** Returns true if the lazy connection has been materialized. */
  def hasConnection = _connection.isDefined

  /** The original auto-commit setting, saved when the connection is first obtained. */
  var originalAutoCommit = true

  /** The original transaction isolation level, saved when the connection is first obtained. */
  var originalTransactionIsolation = java.sql.Connection.TRANSACTION_NONE

  /**
   * Returns the JDBC connection, lazily obtaining it from `connectionFunc` on first access.
   * On first access, disables auto-commit and records the original auto-commit and
   * transaction isolation settings for later restoration.
   */
  def connection: Connection = {
    /*
     * No need to synchronize this since Sessions are not thread safe
     */
    _connection getOrElse {
      val c = connectionFunc()
      try {
        originalAutoCommit = c.getAutoCommit
        if (originalAutoCommit)
          c.setAutoCommit(false)
        originalTransactionIsolation = c.getTransactionIsolation
        _connection = Option(c)
        c
      } catch {
        case e: SQLException =>
          Utils.close(connection)
          throw e
      }
    }
  }

  /**
   * Executes the given function within a transaction. Commits on success, rolls back on
   * failure. Only performs commit/rollback/close if a connection was actually obtained
   * (since this is a lazy session). Restores the original auto-commit and transaction
   * isolation settings before closing the connection.
   */
  def withinTransaction[A](f: () => A): A = {
    var txOk = false
    try {
      val res = this.using[A](f)
      txOk = true
      res
    } catch {
      case e: ControlThrowable => {
        txOk = true
        throw e
      }
    } finally {
      if (hasConnection) {
        try {
          try {
            if (txOk)
              connection.commit
            else
              connection.rollback
          } finally {
            if (originalAutoCommit != connection.getAutoCommit) {
              connection.setAutoCommit(originalAutoCommit)
            }
            connection.setTransactionIsolation(originalTransactionIsolation)
          }
        } catch {
          case e: SQLException => {
            Utils.close(connection)
            if (txOk)
              throw e // if an exception occurred b4 the commit/rollback we don't want to obscure the original exception
          }
        }
        try {
          if (!connection.isClosed)
            connection.close
        } catch {
          case e: SQLException => {
            if (txOk) throw e // if an exception occurred b4 the close we don't want to obscure the original exception
          }
        }
      }
    }
  }

}

/**
 * A session implementation that holds an eagerly-provided JDBC connection. The connection
 * is always available and is used immediately for database operations.
 *
 * @param connection         the JDBC connection for this session
 * @param databaseAdapter    the database adapter for SQL dialect-specific operations
 * @param statisticsListener optional listener for query statistics
 */
class Session(
  val connection: Connection,
  val databaseAdapter: DatabaseAdapter,
  val statisticsListener: Option[StatisticsListener] = None
) extends AbstractSession {

  /** Always true for eager sessions, since the connection is provided at construction time. */
  val hasConnection = true

  /**
   * Executes the given function within a transaction. Disables auto-commit, executes the
   * function, then commits on success or rolls back on failure. Restores the original
   * auto-commit and transaction isolation settings, then closes the connection.
   */
  def withinTransaction[A](f: () => A): A = {
    val originalAutoCommit =
      try {
        val r = connection.getAutoCommit
        if (r)
          connection.setAutoCommit(false)
        r
      } catch {
        case e: SQLException =>
          Utils.close(connection)
          throw e
      }
    val originalTransactionIsolation =
      try {
        connection.getTransactionIsolation
      } catch {
        case e: SQLException => {
          Utils.close(connection)
          throw e
        }
      }
    var txOk = false
    try {
      val res = this.using[A](f)
      txOk = true
      res
    } catch {
      case e: ControlThrowable => {
        txOk = true
        throw e
      }
    } finally {
      try {
        try {
          if (txOk)
            connection.commit
          else
            connection.rollback
        } finally {
          if (originalAutoCommit != connection.getAutoCommit) {
            connection.setAutoCommit(originalAutoCommit)
          }
          connection.setTransactionIsolation(originalTransactionIsolation)
        }
      } catch {
        case e: SQLException => {
          Utils.close(connection)
          if (txOk)
            throw e // if an exception occurred b4 the commit/rollback we don't want to obscure the original exception
        }
      }
      try {
        if (!connection.isClosed)
          connection.close
      } catch {
        case e: SQLException => {
          if (txOk) throw e // if an exception occurred b4 the close we don't want to obscure the original exception
        }
      }
    }
  }

}

/**
 * Base trait for all Squeryl session types. A session encapsulates a JDBC connection,
 * a database adapter, and manages the lifecycle of statements and result sets created
 * during query execution.
 *
 * Sessions are bound to the current thread via `bindToCurrentThread` and unbound via
 * `unbindFromCurrentThread`. The `using` method temporarily binds this session,
 * executes a function, then restores the previous session binding.
 *
 * Tracks all opened `Statement` and `ResultSet` instances for proper cleanup via
 * the `cleanup` method, which closes them and clears thread-local state.
 */
trait AbstractSession {

  /** Returns the JDBC connection for this session. */
  def connection: Connection

  /** Returns true if a JDBC connection is available (always true for eager sessions, conditional for lazy). */
  def hasConnection: Boolean

  /** Executes the given function within a transaction, handling commit/rollback and connection cleanup. */
  protected[squeryl] def withinTransaction[A](f: () => A): A

  /**
   * Binds this session to the current thread, executes the given function, then unbinds
   * and cleans up. If another session was previously bound, it is restored after execution.
   */
  protected[squeryl] def using[A](a: () => A): A = {
    val s = Session.currentSessionOption
    try {
      if (s.isDefined) s.get.unbindFromCurrentThread
      try {
        this.bindToCurrentThread
        val r = a()
        r
      } finally {
        this.unbindFromCurrentThread
        this.cleanup
      }
    } finally {
      if (s.isDefined) s.get.bindToCurrentThread
    }
  }

  def databaseAdapter: DatabaseAdapter

  def statisticsListener: Option[StatisticsListener]

  /** Binds this session to the current thread, making it accessible via `Session.currentSession`. */
  def bindToCurrentThread = Session.currentSession = Some(this)

  /** Removes this session from the current thread's binding. */
  def unbindFromCurrentThread = Session.currentSession = None

  private[this] var _logger: String => Unit = null

  /** Sets the logging function for this session. When set, SQL statements are logged via this function. */
  def logger_=(f: String => Unit) = _logger = f

  /** Sets the logging function for this session (Java-friendly alternative to `logger_=`). */
  def setLogger(f: String => Unit) = _logger = f

  /** Returns true if a logger function has been set for this session. */
  def isLoggingEnabled = _logger != null

  /** Logs the given message if logging is enabled. */
  def log(s: String) = if (isLoggingEnabled) _logger(s)

  /** When true, unclosed statements are logged during cleanup (requires logging to be enabled). */
  var logUnclosedStatements = false

  private[this] val _statements = new ArrayBuffer[Statement]

  private[this] val _resultSets = new ArrayBuffer[ResultSet]

  private[squeryl] def _addStatement(s: Statement) = _statements.append(s)

  private[squeryl] def _addResultSet(rs: ResultSet) = _resultSets.append(rs)

  /**
   * Closes all tracked statements and result sets, clearing the internal buffers.
   * Also clears thread-local state from [[org.squeryl.internals.FieldReferenceLinker]]. Should be called
   * at the end of each unit of work to prevent resource leaks.
   */
  def cleanup = {
    _statements.foreach(s => {
      if (logUnclosedStatements && isLoggingEnabled && !s.isClosed) {
        val stackTrace = Thread.currentThread.getStackTrace.map("at " + _).mkString("\n")
        log("Statement is not closed: " + s + ": " + System.identityHashCode(s) + "\n" + stackTrace)
      }
      Utils.close(s)
    })
    _statements.clear()
    _resultSets.foreach(rs => Utils.close(rs))
    _resultSets.clear()

    FieldReferenceLinker.clearThreadLocalState()
  }

  /**
   * Performs cleanup of all tracked resources and then closes the underlying JDBC
   * connection if one is available.
   */
  def close = {
    cleanup
    if (hasConnection)
      connection.close
  }

}

/**
 * Trait for factories that create new [[AbstractSession]] instances. Implementations
 * provide the connection and adapter configuration for session creation.
 */
trait SessionFactory {
  def newSession: AbstractSession
}

/**
 * Companion object that serves as the global session factory registry. Either
 * `concreteFactory` or `externalTransactionManagementAdapter` must be initialized
 * before using `transaction` or `inTransaction` blocks.
 */
object SessionFactory {

  /**
   * Initializing concreteFactory with a Session creating closure enables the use of
   * the 'transaction' and 'inTransaction' block functions 
   */
  var concreteFactory: Option[() => AbstractSession] = None

  /**
   * Initializing externalTransactionManagementAdapter with a Session creating closure allows to
   * execute Squeryl statements *without* the need of using 'transaction' and 'inTransaction'.
   * The use case for this is to allow Squeryl connection/transactions to be managed by an
   * external framework. In this case Session.cleanupResources *needs* to be called when connections
   * are closed, otherwise statement of resultset leaks can occur. 
   */
  var externalTransactionManagementAdapter: Option[() => Option[AbstractSession]] = None

  def newSession: AbstractSession =
    concreteFactory
      .getOrElse(
        throw new IllegalStateException(
          "org.squeryl.SessionFactory not initialized, SessionFactory.concreteFactory must be assigned a \n" +
            "function for creating new org.squeryl.Session, before transaction can be used.\n" +
            "Alternatively SessionFactory.externalTransactionManagementAdapter can initialized, please refer to the documentation."
        )
      )
      .apply()
}

/**
 * Companion object for session management. Provides thread-local storage for the current
 * session, factory methods for creating sessions, and utility methods for accessing the
 * current session.
 *
 * Sessions are stored in a `ThreadLocal` and must be properly removed (via `unbindFromCurrentThread`
 * or transaction block completion) to avoid polluting thread pools in servlet containers.
 */
object Session {

  /**
   * Note about ThreadLocals: all thread locals should be .removed() before the
   * transaction ends.
   * 
   * Leaving a ThreadLocal inplace after the control returns to the user thread
   * will pollute the users threads and will cause problems for e.g. Tomcat and
   * other servlet engines.
   */
  private[this] val _currentSessionThreadLocal = new ThreadLocal[AbstractSession]

  /** Creates an eager [[Session]] with the given connection and database adapter. */
  def create(c: Connection, a: DatabaseAdapter) =
    new Session(c, a)

  /** Creates a [[LazySession]] that defers connection acquisition until first use. */
  def create(connectionFunc: () => Connection, a: DatabaseAdapter) =
    new LazySession(connectionFunc, a)

  /**
   * Returns the current session if one is bound to this thread, or attempts to obtain
   * one from the external transaction management adapter if configured. Returns `None`
   * if no session is available from either source.
   */
  def currentSessionOption: Option[AbstractSession] = {
    Option(_currentSessionThreadLocal.get) orElse {
      SessionFactory.externalTransactionManagementAdapter flatMap { _.apply() }
    }
  }

  /**
   * Returns the current session bound to this thread. If using external transaction
   * management, delegates to the adapter. Throws `IllegalStateException` if no session
   * is available, which typically indicates a statement was executed outside a
   * transaction block.
   */
  def currentSession: AbstractSession =
    SessionFactory.externalTransactionManagementAdapter match {
      case Some(a) =>
        a.apply() getOrElse org.squeryl.internals.Utils.throwError(
          "SessionFactory.externalTransactionManagementAdapter was unable to supply a Session for the current scope"
        )
      case None =>
        currentSessionOption.getOrElse(
          throw new IllegalStateException(
            "No session is bound to current thread, a session must be created via Session.create \nand bound to the thread via 'work' or 'bindToCurrentThread'\n Usually this error occurs when a statement is executed outside of a transaction/inTrasaction block"
          )
        )
    }

  /** Returns true if a session is currently bound to this thread or available via external management. */
  def hasCurrentSession =
    currentSessionOption.isDefined

  /**
   * Calls `cleanup` on the current session if one exists. This should be called when
   * connections are managed externally (e.g., by a framework) to prevent statement and
   * result set leaks.
   */
  def cleanupResources =
    currentSessionOption foreach (_.cleanup)

  private[squeryl] def currentSession_=(s: Option[AbstractSession]) =
    if (s.isEmpty) {
      _currentSessionThreadLocal.remove()
    } else {
      _currentSessionThreadLocal.set(s.get)
    }

}
