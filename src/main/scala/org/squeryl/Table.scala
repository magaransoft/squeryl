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
package org.squeryl;

import dsl.ast.*
import dsl.{CompositeKey, QueryDsl}
import internals.*

import java.sql.Statement
import logging.StackMarker
import org.squeryl.helpers.Discardable

import collection.mutable.ArrayBuffer

//private [squeryl] object DummySchema extends Schema

/**
 * Represents a database table mapped to a Scala type `T`.
 *
 * `Table` extends [[View]] with full mutation capabilities: insert, update, delete, and
 * insertOrUpdate (upsert). All single-row mutation methods return [[org.squeryl.helpers.Discardable]]
 * to signal that the return value may be safely ignored.
 *
 * Lifecycle callbacks (beforeInsert, afterInsert, beforeUpdate, afterUpdate, beforeDelete,
 * afterDelete) defined in the owning [[Schema]] are automatically invoked by mutation methods.
 *
 * Tables are not instantiated directly; they are created via `table[T]` declarations inside a
 * [[Schema]] definition.
 *
 * @tparam T the row type mapped to this table
 * @param n the table name
 * @param c the runtime class of `T`
 * @param schema the owning schema
 * @param _prefix an optional schema prefix (e.g. database schema name)
 * @param ked an optional [[KeyedEntityDef]] for tables whose rows have a primary key
 * @param optionalFieldsInfo optional metadata about `Option`-typed fields
 */
class Table[T] private[squeryl] (
  n: String,
  c: Class[T],
  val schema: Schema,
  _prefix: Option[String],
  ked: Option[KeyedEntityDef[T, _]],
  optionalFieldsInfo: Option[Map[String, Class[_]]]
) extends View[T](n, c, schema, _prefix, ked, optionalFieldsInfo) {

  private def _dbAdapter = Session.currentSession.databaseAdapter

  /**
   * Inserts a single row into this table and returns the inserted object.
   *
   * If the table's primary key is auto-incremented, the generated key value is read back from
   * the database and set on the returned object. Lifecycle callbacks (beforeInsert, afterInsert)
   * are invoked around the operation, and the row is marked as persisted.
   *
   * @param t the object to insert
   * @return the inserted object (with auto-generated key populated, if applicable)
   * @throws SquerylSQLException when a database error occurs or the insert does not result in
   *                             exactly 1 row
   */
  def insert(t: T): Discardable[T] = StackMarker.lastSquerylStackFrame {

    val o = _callbacks.beforeInsert(t.asInstanceOf[AnyRef])
    val sess = Session.currentSession
    val sw = new StatementWriter(_dbAdapter)
    _dbAdapter.writeInsert(o.asInstanceOf[T], this, sw)

    val st =
      (_dbAdapter.supportsAutoIncrementInColumnDeclaration, posoMetaData.primaryKey) match {
        case (true, a: Any) => sess.connection.prepareStatement(sw.statement, Statement.RETURN_GENERATED_KEYS)
        case (false, Some(Left(pk: FieldMetaData))) => {
          val autoIncPk = new Array[String](1)
          autoIncPk(0) = pk.columnName
          sess.connection.prepareStatement(sw.statement, autoIncPk)
        }
        case a: Any => sess.connection.prepareStatement(sw.statement)
      }

    try {
      val cnt = _dbAdapter.executeUpdateForInsert(sess, sw, st)

      if (cnt != 1)
        throw SquerylSQLException("failed to insert.  Expected 1 row, got " + cnt)

      posoMetaData.primaryKey match {
        case Some(Left(pk: FieldMetaData)) =>
          if (pk.isAutoIncremented) {
            val rs = st.getGeneratedKeys
            try {
              assert(
                rs.next,
                "getGeneratedKeys returned no rows for the auto incremented\n" +
                  " primary key of table '" + name + "' JDBC3 feature might not be supported, \n or" +
                  " column might not be defined as auto increment"
              )
              pk.setFromResultSet(o, rs, 1)
            } finally {
              rs.close
            }
          }
        case a: Any => {}
      }
    } finally {
      st.close
    }

    val r = _callbacks.afterInsert(o).asInstanceOf[T]

    _setPersisted(r)

    r
  }

//  def insert(t: Query[T]) = org.squeryl.internals.Utils.throwError("not implemented")

  /**
   * Inserts multiple rows into this table using JDBC batch execution.
   *
   * Auto-incremented fields are excluded from the batch insert statement. Lifecycle callbacks
   * (beforeInsert, afterInsert) are invoked for each element. After successful insertion, all
   * rows are marked as persisted.
   *
   * @param e the collection of objects to insert
   */
  def insert(e: Iterable[T]): Unit =
    _batchedUpdateOrInsert(
      e,
      t => posoMetaData.fieldsMetaData.filter(fmd => !fmd.isAutoIncremented && fmd.isInsertable),
      true,
      false
    )

  /**
   * Performs a batched insert or update using JDBC batch execution.
   *
   * Prepares a single statement from the first element, then adds all subsequent elements
   * as batches. Lifecycle callbacks are invoked for each element. When `checkOCC` is true,
   * a [[StaleUpdateException]] is thrown if any batch entry affects 0 rows (indicating an
   * optimistic concurrency conflict).
   *
   * @param e the collection of objects to insert or update
   * @param fmdCallback a function that returns the [[internals.FieldMetaData]] to include in the
   *                    statement for a given row
   * @param isInsert true for insert operations, false for update operations
   * @param checkOCC true to enable optimistic concurrency control checks on the batch results
   */
  private def _batchedUpdateOrInsert(
    e: Iterable[T],
    fmdCallback: T => Iterable[FieldMetaData],
    isInsert: Boolean,
    checkOCC: Boolean
  ): Unit = {

    val it = e.iterator

    if (it.hasNext) {

      val e0 = it.next()
      val fmds = fmdCallback(e0)
      val sess = Session.currentSession
      val dba = _dbAdapter
      val sw = new StatementWriter(dba)
      val forAfterUpdateOrInsert = new ArrayBuffer[AnyRef]

      if (isInsert) {
        val z = _callbacks.beforeInsert(e0.asInstanceOf[AnyRef])
        forAfterUpdateOrInsert += z
        dba.writeInsert(z.asInstanceOf[T], this, sw)
      } else {
        val z = _callbacks.beforeUpdate(e0.asInstanceOf[AnyRef])
        forAfterUpdateOrInsert += z
        dba.writeUpdate(z.asInstanceOf[T], this, sw, checkOCC)
      }

      if (sess.isLoggingEnabled)
        sess.log("Performing batched " + (if (isInsert) "insert" else "update") + " with " + sw.statement)

      val st = sess.connection.prepareStatement(sw.statement)

      try {
        dba.fillParamsInto(sw.params, st)
        st.addBatch

        var updateCount = 1

        while (it.hasNext) {
          val eN0 = it.next().asInstanceOf[AnyRef]
          val eN =
            if (isInsert)
              _callbacks.beforeInsert(eN0)
            else
              _callbacks.beforeUpdate(eN0)

          forAfterUpdateOrInsert += eN

          var idx = 1
          fmds.foreach(fmd => {
            dba.setParamInto(st, FieldStatementParam(eN, fmd), idx)
            idx += 1
          })
          st.addBatch
          updateCount += 1
        }

        val execResults = st.executeBatch

        if (checkOCC)
          for (b <- execResults)
            if (b == 0) {
              val updateOrInsert = if (isInsert) "insert" else "update"
              throw new StaleUpdateException(
                "Attempted to " + updateOrInsert + " stale object under optimistic concurrency control"
              )
            }
      } finally {
        st.close
      }

      for (a <- forAfterUpdateOrInsert)
        if (isInsert) {
          _setPersisted(_callbacks.afterInsert(a).asInstanceOf[T])
        } else
          _callbacks.afterUpdate(a)

    }
  }

  /**
   * Updates a single row without optimistic concurrency control (OCC) checks.
   *
   * Unlike [[update]], this method will not throw a [[StaleUpdateException]] when the row's
   * OCC version does not match. Use this when you intentionally want to overwrite regardless
   * of concurrent modifications.
   *
   * @param o the object to update (must be a keyed entity)
   * @param ked the implicit keyed entity definition for `T`
   * @throws SquerylSQLException when a database error occurs or the update does not result in
   *                             exactly 1 row
   */
  def forceUpdate[K](o: T)(implicit ked: KeyedEntityDef[T, _]) =
    _update(o, false, ked)

  /**
   * Updates a single row in this table, identified by its primary key.
   *
   * If the entity uses optimistic concurrency control (OCC), a [[StaleUpdateException]] is
   * thrown when the row's version number does not match the database. Lifecycle callbacks
   * (beforeUpdate, afterUpdate) are invoked around the operation.
   *
   * @param o the object to update (must be a keyed entity)
   * @param ked the implicit keyed entity definition for `T`
   * @throws SquerylSQLException when a database error occurs or the update does not result in
   *                             exactly 1 row
   * @throws StaleUpdateException when the row has been modified by another transaction (OCC conflict)
   */
  def update(o: T)(implicit ked: KeyedEntityDef[T, _]): Unit =
    _update(o, true, ked)

  /**
   * Updates multiple rows using JDBC batch execution with optimistic concurrency control.
   *
   * @param o the collection of objects to update
   * @param ked the implicit keyed entity definition for `T`
   */
  def update(o: Iterable[T])(implicit ked: KeyedEntityDef[T, _]): Unit =
    _update(o, ked.isOptimistic)

  /**
   * Updates multiple rows using JDBC batch execution without optimistic concurrency control checks.
   *
   * @param o the collection of objects to update
   * @param ked the implicit keyed entity definition for `T`
   */
  def forceUpdate(o: Iterable[T])(implicit ked: KeyedEntityDef[T, _]): Unit =
    _update(o, ked.isOptimistic)

  /**
   * Internal single-row update implementation.
   *
   * Invokes beforeUpdate/afterUpdate lifecycle callbacks and optionally checks for optimistic
   * concurrency control violations.
   *
   * @param o the object to update
   * @param checkOCC whether to check for OCC violations
   * @param ked the keyed entity definition for `T`
   */
  private def _update(o: T, checkOCC: Boolean, ked: KeyedEntityDef[T, _]) = {

    val dba = Session.currentSession.databaseAdapter
    val sw = new StatementWriter(dba)
    val o0 = _callbacks.beforeUpdate(o.asInstanceOf[AnyRef]).asInstanceOf[T]
    dba.writeUpdate(o0, this, sw, checkOCC)

    val cnt = dba.executeUpdateAndCloseStatement(Session.currentSession, sw)

    if (cnt != 1) {
      if (checkOCC && posoMetaData.isOptimistic) {
        val version = posoMetaData.optimisticCounter.get.getNativeJdbcValue(o.asInstanceOf[AnyRef])
        throw new StaleUpdateException(
          "Object " + prefixedName + "(id=" + ked.getId(o) + ", occVersionNumber=" + version +
            ") has become stale, it cannot be updated under optimistic concurrency control"
        )
      } else
        throw SquerylSQLException("failed to update.  Expected 1 row, got " + cnt)
    }

    _callbacks.afterUpdate(o0.asInstanceOf[AnyRef])
  }

  /**
   * Internal batch update implementation.
   *
   * Builds the field metadata list (updatable fields + primary key fields + OCC counter) and
   * delegates to [[_batchedUpdateOrInsert]].
   *
   * @param e the collection of objects to update
   * @param checkOCC whether to check for OCC violations on each batch entry
   */
  private def _update(e: Iterable[T], checkOCC: Boolean): Unit = {

    def buildFmds(t: T): Iterable[FieldMetaData] = {
      val pkList = posoMetaData.primaryKey
        .getOrElse(
          org.squeryl.internals.Utils
            .throwError("method was called with " + posoMetaData.clasz.getName + " that is not a KeyedEntity[]")
        )
        .fold(
          pkMd => List(pkMd),
          pkGetter => {
            // Just for side-effect...
            var fields: Option[List[FieldMetaData]] = None
            Utils.createQuery4WhereClause(
              this,
              (t0: T) => {
                val ck = pkGetter.invoke(t0).asInstanceOf[CompositeKey]

                fields = Some(ck._fields.toList)

                new EqualityExpression(
                  InternalFieldMapper.intTEF.createConstant(1),
                  InternalFieldMapper.intTEF.createConstant(1)
                )
              }
            )

            fields getOrElse (internals.Utils.throwError("No PK fields found"))
          }
        )

      List(
        posoMetaData.fieldsMetaData
          .filter(fmd => !fmd.isIdFieldOfKeyedEntity && !fmd.isOptimisticCounter && fmd.isUpdatable)
          .toList,
        pkList,
        posoMetaData.optimisticCounter.toList
      ).flatten
    }

    _batchedUpdateOrInsert(e, buildFmds _, false, checkOCC)
  }

  /**
   * Executes a partial update using a DSL-style update statement, allowing updates to specific
   * columns with arbitrary WHERE conditions rather than updating an entire row by primary key.
   *
   * Example usage:
   * {{{
   * table.update(t =>
   *   where(t.id === 5)
   *   set(t.name := "newName")
   * )
   * }}}
   *
   * @param s a function that takes a sample instance of `T` and returns an [[dsl.ast.UpdateStatement]]
   * @return the number of rows affected
   */
  def update(s: T => UpdateStatement): Discardable[Int] = {

    val vxn = new ViewExpressionNode(this)
    vxn.sample = posoMetaData.createSample(vxn)
    val us = s(vxn.sample)
    vxn.parent = Some(us)

    var idGen = 0
    us.visitDescendants((node, parent, i) => {

      if (node.parent.isEmpty)
        node.parent = parent

      node match {
        case nxn: UniqueIdInAliaseRequired =>
          nxn.uniqueId = Some(idGen)
          idGen += 1
        case _ =>
      }
    })

    vxn.uniqueId = Some(idGen)

    val dba = _dbAdapter
    val sw = new StatementWriter(dba)
    dba.writeUpdate(this, us, sw)
    dba.executeUpdateAndCloseStatement(Session.currentSession, sw)
  }

  /**
   * Deletes all rows matched by the given query.
   *
   * The query's WHERE clause is used to determine which rows to delete from this table.
   *
   * @param q a query whose results identify the rows to delete
   * @return the number of rows deleted
   */
  def delete(q: Query[T]): Discardable[Int] = {

    val queryAst = q.ast.asInstanceOf[QueryExpressionElements]
    queryAst.inhibitAliasOnSelectElementReference = true

    val sw = new StatementWriter(_dbAdapter)
    _dbAdapter.writeDelete(this, queryAst.whereClause, sw)

    _dbAdapter.executeUpdateAndCloseStatement(Session.currentSession, sw)
  }

  /**
   * Deletes all rows matching the given WHERE clause.
   *
   * This is a convenience method that constructs a query from the WHERE clause and delegates
   * to [[delete(Query)]].
   *
   * @param whereClause a function producing a logical condition for row selection
   * @param dsl the implicit QueryDsl
   * @return the number of rows deleted
   */
  def deleteWhere(whereClause: T => LogicalBoolean)(implicit dsl: QueryDsl): Discardable[Int] =
    delete(dsl.from(this)(t => dsl.where(whereClause(t)).select(t)))

  /**
   * Deletes the row with the given primary key value.
   *
   * Lifecycle callbacks (beforeDelete, afterDelete) are invoked if registered. An assertion
   * verifies that at most one row was deleted (when supported by the database adapter).
   *
   * @param k the primary key value identifying the row to delete
   * @param ked the implicit keyed entity definition for `T`
   * @param dsl the implicit QueryDsl
   * @param toCanLookup implicit conversion enabling key lookup
   * @return true if a row was deleted, false if no matching row was found
   */
  def delete[K](
    k: K
  )(implicit ked: KeyedEntityDef[T, K], dsl: QueryDsl, toCanLookup: K => CanLookup): Discardable[Boolean] = {
    import dsl._
    val q = from(this)(a =>
      dsl.where {
        FieldReferenceLinker
          .createEqualityExpressionWithLastAccessedFieldReferenceAndConstant(ked.getId(a), k, toCanLookup(k))
      } select (a)
    )

    lazy val z = q.headOption

    if (_callbacks.hasBeforeDelete) {
      z.map(x => _callbacks.beforeDelete(x.asInstanceOf[AnyRef]))
    }

    val deleteCount = this.delete(q)

    if (_callbacks.hasAfterDelete) {
      z.map(x => _callbacks.afterDelete(x.asInstanceOf[AnyRef]))
    }

    if (Session.currentSessionOption forall { ses => ses.databaseAdapter.verifyDeleteByPK })
      assert(
        deleteCount <= 1,
        "Query :\n" + q.dumpAst + "\nshould have deleted at most 1 row but has deleted " + deleteCount
      )
    deleteCount > 0
  }

  /**
   * Inserts the object if it is not yet persisted, or updates it if it is.
   *
   * Persistence status is determined by [[KeyedEntityDef.isPersisted]]. This provides a
   * convenient upsert-like operation based on the entity's in-memory persistence tracking
   * rather than a database-level MERGE/UPSERT.
   *
   * @param o the object to insert or update
   * @param ked the implicit keyed entity definition for `T`
   * @return the object after insertion or update
   * @throws SquerylSQLException when a database error occurs or the operation does not result in
   *                             exactly 1 row
   */
  def insertOrUpdate(o: T)(implicit ked: KeyedEntityDef[T, _]): Discardable[T] = {
    if (ked.isPersisted(o))
      update(o)
    else
      insert(o)
    o
  }
}
