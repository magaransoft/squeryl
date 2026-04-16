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

import dsl.ast.ViewExpressionNode
import dsl.{TypedExpression, QueryDsl}
import internals._
import java.sql.ResultSet

/**
 * Represents a read-only database view or table mapped to a Scala type `T`.
 *
 * `View` provides query capabilities (via [[Queryable]]) including [[lookup]] by primary key,
 * [[get]] (lookup that throws on missing), and [[allRows]]. For mutable operations (insert,
 * update, delete), use [[Table]] instead.
 *
 * Views hold POSO (Plain Old Scala Object) metadata for mapping between Scala objects and
 * database result sets. They also manage lifecycle callbacks (e.g. afterSelect) delegated
 * from the owning [[Schema]].
 *
 * @tparam T the row type mapped to this view
 * @param _name the view/table name
 * @param classOfT the runtime class of `T`
 * @param schema the owning schema
 * @param _prefix an optional schema prefix
 * @param ked an optional [[KeyedEntityDef]] for views whose rows have a primary key
 * @param optionalFieldsInfo optional metadata about `Option`-typed fields
 */
class View[T] private[squeryl] (
  _name: String,
  private[squeryl] val classOfT: Class[T],
  schema: Schema,
  _prefix: Option[String],
  val ked: Option[KeyedEntityDef[T, _]],
  val optionalFieldsInfo: Option[Map[String, Class[_]]]
) extends Queryable[T] {

//2.9.x approach for LyfeCycle events :
//  private [squeryl] var _callbacks: PosoLifecycleEventListener = NoOpPosoLifecycleEventListener

////2.8.x approach for LyfeCycle events :
  private[squeryl] lazy val _callbacks =
    schema._callbacks.getOrElse(this, NoOpPosoLifecycleEventListener)

  /** Returns the table/view name after applying the schema's naming convention transformation. */
  def name = schema.tableNameFromClassName(_name)

  /** Returns the schema prefix for this view, falling back to the owning schema's name if not explicitly set. */
  def prefix: Option[String] =
    if (_prefix.isDefined)
      _prefix
    else
      schema.name

  /** Returns the fully qualified name of this view, including the schema prefix if defined (e.g. "myschema.MyTable"). */
  def prefixedName =
    if (prefix.isDefined)
      prefix.get + "." + name
    else
      name

  /**
   * Suppose you have : prefix.MyTable
   * myTable.prefixedPrefixedName("z") will yield : prefix.zMyTable
   * used for creating names for objects derived from a table, ex.: a sequence 
   */
  def prefixedPrefixedName(s: String) =
    if (prefix.isDefined)
      prefix.get + "." + s + name
    else
      s + name

  private[squeryl] def findFieldMetaDataForProperty(name: String) = posoMetaData.findFieldMetaDataForProperty(name)

  /** Metadata describing the mapping between Scala object fields and database columns for type `T`. */
  val posoMetaData = new PosoMetaData(classOfT, schema, this)

  private[squeryl] def allFieldsMetaData: Iterable[FieldMetaData] = posoMetaData.fieldsMetaData

  protected val _setPersisted =
    if (classOf[PersistenceStatus].isAssignableFrom(classOfT))
      (t: T) => t.asInstanceOf[PersistenceStatus]._isPersisted = true
    else
      (t: T) => {}

  private[this] val _posoFactory =
    FieldMetaData.factory.createPosoFactory(posoMetaData)

  private[squeryl] def _createInstanceOfRowObject =
    _posoFactory()

  private[squeryl] def give(resultSetMapper: ResultSetMapper, resultSet: ResultSet): T = {

    var o = _callbacks.create

    if (o == null)
      o = _createInstanceOfRowObject

    resultSetMapper.map(o, resultSet);
    val t = o.asInstanceOf[T]
    _setPersisted(t)
    _callbacks.afterSelect(t.asInstanceOf[AnyRef]).asInstanceOf[T]
  }

  /**
   * Looks up a single row by its primary key.
   *
   * Constructs and executes a query with a WHERE clause matching the given key value.
   * Returns `None` if no matching row is found.
   *
   * @tparam K the primary key type
   * @param k the primary key value to look up
   * @param ked the implicit keyed entity definition for `T`
   * @param dsl the implicit QueryDsl
   * @param toCanLookup implicit conversion enabling key-based lookup
   * @return `Some(row)` if found, `None` otherwise
   */
  def lookup[K](k: K)(implicit ked: KeyedEntityDef[T, K], dsl: QueryDsl, toCanLookup: K => CanLookup): Option[T] = {
    // TODO: find out why scalac won't let dsl be passed to another method
    import dsl._

    val q = from(this)(a =>
      dsl.where {
        FieldReferenceLinker
          .createEqualityExpressionWithLastAccessedFieldReferenceAndConstant(ked.getId(a), k, toCanLookup(k))
      } select (a)
    )

    val it = q.iterator

    if (it.hasNext) {
      val ret = Some(it.next())
      // Forces statement to be closed.
      it.hasNext
      ret
    } else
      None
  }

  /**
   * Retrieves a single row by its primary key, throwing if not found.
   *
   * This is a non-optional variant of [[lookup]] that throws a `NoSuchElementException`
   * when no row matches the given key.
   *
   * @tparam K the primary key type
   * @param k the primary key value to look up
   * @param ked the implicit keyed entity definition for `T`
   * @param dsl the implicit QueryDsl
   * @param toCanLookup implicit conversion enabling key-based lookup
   * @return the matching row
   * @throws NoSuchElementException if no row with the given key exists
   */
  def get[K](k: K)(implicit ked: KeyedEntityDef[T, K], dsl: QueryDsl, toCanLookup: K => CanLookup): T =
    lookup(k).getOrElse(throw new NoSuchElementException("Found no row with key '" + k + "' in " + name + "."))

  /**
   * Returns all rows in this view/table as an `Iterable`.
   *
   * Executes a `SELECT * FROM table` query with no WHERE clause.
   *
   * @param dsl the implicit QueryDsl
   * @return all rows in this view/table
   */
  def allRows(implicit dsl: QueryDsl): Iterable[T] = {
    import dsl._
    dsl.queryToIterable(from(this)(a => select(a)))
  }

  /** Creates a new AST node representing this view in a query expression tree. */
  def viewExpressionNode: ViewExpressionNode[T] = new ViewExpressionNode[T](this)
}

/**
 * A sealed marker trait used to constrain the types that can serve as lookup keys in
 * [[View.lookup]] and [[View.get]] operations.
 *
 * Implicit conversions from supported key types to `CanLookup` are provided by the
 * framework, enabling both simple keys (e.g. `Long`, `String`) and composite keys.
 */
sealed trait CanLookup

private[squeryl] case object CompositeKeyLookup extends CanLookup

private[squeryl] case object UnknownCanLookup extends CanLookup

private[squeryl] case class SimpleKeyLookup[T](convert: T => TypedExpression[T, _]) extends CanLookup
