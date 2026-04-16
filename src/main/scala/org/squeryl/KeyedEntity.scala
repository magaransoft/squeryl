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

import annotations.Transient
import java.sql.SQLException

/**
 * Defines the primary key behavior for an entity type `A` with key type `K`. This type class
 * enables Squeryl to perform key-based operations (lookup, delete, update) on entities that
 * do not extend [[KeyedEntity]] directly, by providing the key extraction and persistence
 * status logic externally via an implicit instance.
 *
 * When an entity extends [[KeyedEntity]] directly, a default `KeyedEntityDef` is provided
 * automatically. For entities that cannot extend the trait (e.g., third-party classes),
 * an implicit `KeyedEntityDef` can be defined in scope.
 *
 * @tparam A the entity type
 * @tparam K the primary key type
 */
@scala.annotation.implicitNotFound(msg =
  "The method requires an implicit org.squeryl.KeyedEntityDef[${A}, ${K}] in scope, or that it extends the trait KeyedEntity[${K}]"
)
trait KeyedEntityDef[-A, K] extends OptionalKeyedEntityDef[A, K] {

  /** Extracts the primary key value from the given entity instance. */
  def getId(a: A): K

  /**
   * returns true if the given instance has been persisted
   */
  def isPersisted(a: A): Boolean

  /**
   * the (Scala) property/field name of the id 
   */
  def idPropertyName: String

  /**
   * the counter field name for OCC, None to disable OCC (optimistic concurrency control)
   */
  def optimisticCounterPropertyName: Option[String] = None

  private[squeryl] def isOptimistic = optimisticCounterPropertyName.isDefined

  /**
   * fulfills the contract of OptionalKeyedEntityDef
   */
  final def keyedEntityDef: Option[KeyedEntityDef[A, K]] = Some(this)
}

/**
 * A type class that optionally provides a [[KeyedEntityDef]] for an entity type. Returns
 * `Some(keyedEntityDef)` when the entity has a defined primary key, or `None` otherwise.
 * Used internally to support operations that work with both keyed and non-keyed entities.
 *
 * @tparam A the entity type
 * @tparam K the primary key type
 */
trait OptionalKeyedEntityDef[-A, K] {

  /** Returns the [[KeyedEntityDef]] for this entity type, or `None` if not available. */
  def keyedEntityDef: Option[KeyedEntityDef[A, K]]
}

/**
 *  For use with View[A] or Table[A], when A extends KeyedEntity[K],
 * lookup and delete by key become implicitly available
 * Example :
 *
 * class Peanut(weight: Float) extends KeyedEntity[Long]
 * val peanutJar = Table[Peanut]
 *
 * Since Peanut extends KeyedEntity the delete(l:Long)
 * method is available
 *  
 * def removePeanut(idOfThePeanut: Long) =
 *   peanutJar.delete(idOfThePeanut)
 *
 * And lookup by id is also implicitly available :
 * 
 * peanutJar.lookup(idOfThePeanut)
 *
 */

/**
 * A convenience trait for entities with a primary key of type `K`. Entities that extend
 * this trait automatically get a [[KeyedEntityDef]] implicit, enabling key-based operations
 * such as `lookup`, `delete`, and `update` on the containing `Table`.
 *
 * Provides equality and hash code implementations based on the primary key when the entity
 * has been persisted. Unpersisted entities fall back to reference equality.
 *
 * @tparam K the primary key type
 */
trait KeyedEntity[K] extends PersistenceStatus {

  /** The primary key value for this entity. */
  def id: K

  override def hashCode =
    if (isPersisted)
      id.hashCode
    else
      super.hashCode

  override def equals(z: Any): Boolean = {
    if (z == null)
      return false
    val ar = z.asInstanceOf[AnyRef]
    if (!ar.getClass.isAssignableFrom(this.getClass))
      false
    else if (isPersisted)
      id == ar.asInstanceOf[KeyedEntity[K]].id
    else
      super.equals(z)
  }
}

/**
 * Tracks whether an entity instance has been persisted to the database. The `_isPersisted`
 * flag is set internally by Squeryl after successful insert or when an entity is loaded
 * from a query result. This flag is transient and not serialized.
 */
trait PersistenceStatus {

  @transient
  @Transient
  private[squeryl] var _isPersisted = false

  /** Returns true if this entity has been persisted to the database. */
  def isPersisted: Boolean = _isPersisted
}

/**
 * A [[KeyedEntity]] variant where the primary key is accessed indirectly through a
 * field of a different type. Useful when the entity's `id` is derived from or wraps
 * another field.
 *
 * @tparam K the primary key type
 * @tparam T the type of the underlying id field
 */
trait IndirectKeyedEntity[K, T] extends KeyedEntity[K] {

  /** The underlying field from which the primary key is derived. */
  def idField: T
}

/**
 * Mixin trait that enables optimistic concurrency control (OCC) for a [[KeyedEntity]].
 * When mixed in, Squeryl will include a version number check in UPDATE statements.
 * If the version in the database does not match the expected version, a
 * [[StaleUpdateException]] is thrown, indicating that another transaction has modified
 * the row since it was read.
 *
 * The `occVersionNumber` field is automatically incremented on each successful update.
 */
trait Optimistic {
  self: KeyedEntity[_] =>

  /** The optimistic concurrency control version counter. Starts at 0 and is incremented on each update. */
  protected val occVersionNumber = 0
}

/**
 * Companion object for [[SquerylSQLException]], providing factory methods that wrap
 * `java.sql.SQLException` with additional context messages.
 */
object SquerylSQLException {
  def apply(message: String, cause: SQLException) =
    new SquerylSQLException(message, Some(cause))
  def apply(message: String) =
    new SquerylSQLException(message, None)
}

/**
 * A runtime exception that wraps a `java.sql.SQLException` with additional Squeryl-specific
 * context. Provides a covariant `getCause` that returns `SQLException` directly for
 * convenient access to the underlying database error.
 *
 * @param message descriptive error message including Squeryl context
 * @param cause   the underlying SQL exception, if any
 */
class SquerylSQLException(message: String, cause: Option[SQLException])
    extends RuntimeException(message, cause.orNull) {
  // Overridden to provide covariant return type as a convenience
  override def getCause: SQLException = cause.orNull
}

/**
 * Thrown when an optimistic concurrency control (OCC) check fails during an UPDATE.
 * This indicates that the row was modified by another transaction between the time
 * it was read and the time the update was attempted. The entity mixes in the
 * [[Optimistic]] trait to enable this behavior.
 *
 * @param message descriptive error message including the entity type and key
 */
class StaleUpdateException(message: String) extends RuntimeException(message)

/** Marker trait for members that are part of an entity's relation graph. */
trait EntityMember {

  /** Returns a query for the root entity of type `B` in this relation. */
  def entityRoot[B]: Query[B]
}

/**
 * Represents a referential integrity action (e.g. CASCADE, RESTRICT) applied to a
 * foreign key constraint on a specific event (UPDATE or DELETE).
 */
trait ReferentialAction {

  /** The SQL event name this action applies to (e.g. "update" or "delete"). */
  def event: String

  /** The SQL action token (e.g. "cascade", "restrict", "no action", "set null"). */
  def action: String
}

/**
 * Declares a foreign key constraint between two tables. Created internally during schema
 * definition when `oneToManyRelation` or `manyToManyRelation` is used. Public methods
 * require an implicit `Schema` to ensure they are only called during schema construction.
 *
 * @param idWithinSchema a unique identifier for this foreign key within its schema
 * @param foreignKeyColumnName the column name of the foreign key
 * @param referencedPrimaryKey the column name of the referenced primary key
 */
class ForeignKeyDeclaration(
  val idWithinSchema: Int,
  val foreignKeyColumnName: String,
  val referencedPrimaryKey: String
) {

  private[this] var _referentialActions: Option[(Option[ReferentialAction], Option[ReferentialAction])] = None

  private[squeryl] def _isActive =
    _referentialActions.isDefined

  private[squeryl] def _referentialAction1: Option[ReferentialAction] =
    _referentialActions.get._1

  private[squeryl] def _referentialAction2: Option[ReferentialAction] =
    _referentialActions.get._2

  /**
   * Causes the foreign key to have no constraint 
   */
  def unConstrainReference()(implicit ev: Schema) =
    _referentialActions = None

  /**
   * Will cause a foreign key constraint to be created at schema creation time :
   * alter table <tableOfForeignKey> add foreign key (<foreignKey>) references <tableOfPrimaryKey>(<primaryKey>)
   */
  def constrainReference()(implicit ev: Schema) =
    _referentialActions = Some((None, None))

  /**
   * Does the same as constrainReference, plus adds a ReferentialAction (ex.: foreignKeyDeclaration.constrainReference(onDelete cascade)) 
   */
  def constrainReference(a1: ReferentialAction)(implicit ev: Schema) =
    _referentialActions = Some((Some(a1), None))

  /**
   * Does the same as constrainReference, plus adds two ReferentialActions
   * (ex.: foreignKeyDeclaration.constrainReference(onDelete cascade, onUpdate restrict)) 
   */
  def constrainReference(a1: ReferentialAction, a2: ReferentialAction)(implicit ev: Schema) =
    _referentialActions = Some((Some(a1), Some(a2)))
}
