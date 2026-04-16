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

import org.squeryl.{ForeignKeyDeclaration, Query, Table}

import collection.mutable.{ArrayBuffer, HashMap}
import org.squeryl.KeyedEntityDef
import org.squeryl.helpers.Discardable

/**
 * Base trait for all relation types in Squeryl's ORM layer. A relation links two
 * tables together, providing access to both the left and right sides.
 *
 * @tparam L the entity type of the left (or "one") side table
 * @tparam R the entity type of the right (or "many") side table
 */
trait Relation[L, R] {

  /** The table on the left side of this relation. */
  def leftTable: Table[L]

  /** The table on the right side of this relation. */
  def rightTable: Table[R]
}

/**
 * Represents a one-to-many relationship between entity type `O` (the "one" side)
 * and entity type `M` (the "many" side). The relationship is defined by a foreign key
 * on the "many" table that references the primary key of the "one" table.
 *
 * Provides methods to navigate the relationship from either side:
 *  - `left(o)` returns a [[OneToMany]] query for all `M` entities associated with `o`
 *  - `right(m)` returns a [[ManyToOne]] query for the single `O` entity that `m` belongs to
 *
 * Stateful variants (`leftStateful`, `rightStateful`) cache results in memory for
 * repeated access without re-querying the database.
 *
 * @tparam O the entity type on the "one" side
 * @tparam M the entity type on the "many" side
 */
trait OneToManyRelation[O, M] extends Relation[O, M] {

  /** The foreign key declaration governing this relationship, used for DDL generation. */
  def foreignKeyDeclaration: ForeignKeyDeclaration

  /** Returns a [[OneToMany]] queryable collection of all `M` entities associated with the given `O` instance. */
  def left(leftSide: O): OneToMany[M]

  /** Returns a [[StatefulOneToMany]] that eagerly loads and caches the "many" side in memory. */
  def leftStateful(leftSide: O) = new StatefulOneToMany[M](left(leftSide))

  /** Returns a [[ManyToOne]] queryable reference to the single `O` entity that owns the given `M` instance. */
  def right(rightSide: M): ManyToOne[O]

  /** Returns a [[StatefulManyToOne]] that eagerly loads and caches the "one" side in memory. */
  def rightStateful(rightSide: M) = new StatefulManyToOne[O](right(rightSide))
}

/**
 * An in-memory cached view of the "many" side of a one-to-many relationship. Eagerly
 * loads all associated entities on construction and keeps them in an internal buffer.
 * Mutations (associate, deleteAll) update both the database and the local cache.
 *
 * Call `refresh` to re-synchronize the cache with the database.
 *
 * @tparam M the entity type on the "many" side
 */
class StatefulOneToMany[M](val relation: OneToMany[M]) extends Iterable[M] {

  private[this] val _buffer = new ArrayBuffer[M]

  refresh

  /** Clears the local cache and reloads all associated entities from the database. */
  def refresh = {
    _buffer.clear()
    for (m <- relation.iterator.toSeq)
      _buffer.append(m)
  }

  def iterator = _buffer.iterator

  /** Associates `m` with the "one" side, persists it, and adds it to the local cache. */
  def associate(m: M) = {
    relation.associate(m)
    _buffer.append(m)
    m
  }

  /** Deletes all associated entities from the database and clears the local cache. */
  def deleteAll: Int = {
    val r = relation.deleteAll
    _buffer.clear()
    r
  }
}

/**
 * An in-memory cached view of the "one" side of a many-to-one relationship. Eagerly
 * loads the parent entity on construction and caches it locally. Mutations (assign, delete)
 * update both the database and the local cache.
 *
 * Call `refresh` to re-synchronize the cache with the database.
 *
 * @tparam O the entity type on the "one" (parent) side
 */
class StatefulManyToOne[O](val relation: ManyToOne[O]) {

  private[this] var _one: Option[O] = None

  refresh

  /** Reloads the parent entity from the database into the local cache. */
  def refresh =
    _one = relation.iterator.toSeq.headOption

  /** Returns the cached parent entity, or `None` if no parent is assigned. */
  def one = _one

  /** Assigns the foreign key to reference `o` and updates the local cache. Does not persist; call on the relation to persist. */
  def assign(o: O) = {
    relation.assign(o)
    _one = Some(o)
    o
  }

  /** Deletes the parent association and clears the local cache. */
  def delete = {
    val b = relation.delete
    _one = None
    b
  }
}

/**
 * Represents a many-to-many relationship between entity types `L` and `R`, mediated
 * by an association entity of type `A`. The association table (which this trait is
 * mixed into as `Table[A]`) holds foreign keys to both the left and right tables.
 *
 * Provides methods to navigate the relationship from either side:
 *  - `left(l)` returns a [[ManyToMany]] query for all `R` entities associated with `l`
 *  - `right(r)` returns a [[ManyToMany]] query for all `L` entities associated with `r`
 *
 * Stateful variants cache results in memory for repeated access without re-querying.
 *
 * @tparam L the entity type on the left side
 * @tparam R the entity type on the right side
 * @tparam A the association entity type (the "join table" entity)
 */
trait ManyToManyRelation[L, R, A] extends Relation[L, R] {
  self: Table[A] =>

  /** The association table itself (this table). */
  def thisTable: Table[A]

  /** The foreign key declaration for the left side of the relationship. */
  def leftForeignKeyDeclaration: ForeignKeyDeclaration

  /** The foreign key declaration for the right side of the relationship. */
  def rightForeignKeyDeclaration: ForeignKeyDeclaration

  /** Returns a [[ManyToMany]] queryable collection of all `R` entities associated with the given `L` instance. */
  def left(leftSide: L): ManyToMany[R, A]

  /** Returns a [[StatefulManyToMany]] that eagerly loads and caches the right side in memory. */
  def leftStateful(leftSide: L) = new StatefulManyToMany[R, A](left(leftSide))

  /** Returns a [[ManyToMany]] queryable collection of all `L` entities associated with the given `R` instance. */
  def right(rightSide: R): ManyToMany[L, A]

  /** Returns a [[StatefulManyToMany]] that eagerly loads and caches the left side in memory. */
  def rightStateful(rightSide: R) = new StatefulManyToMany[L, A](right(rightSide))
}

/**
 * This trait is what is referred by both the left and right side of a manyToMany relation.
 * Type parameters are :
 *   O: the type at the "other" side of the relation
 *   A: the association type i.e. the entity in the "middle" of the relation
 *
 *  Object mapping to the "middle" entity are called "association objects"
 *
 * this trait extends Query[O] and can be queried against like a normal query.
 *
 * Note that this trait is used on both "left" and "right" sides of the relation,
 * but in a given relation  
 */
trait ManyToMany[O, A] extends Query[O] {

  /** The [[KeyedEntityDef]] for the entity type on the "other" side of the relation. */
  def kedL: KeyedEntityDef[O, _]

  /**
   * @param a: the association object
   * 
   * Sets the foreign keys of the association object to the primary keys of the left and right side,
   * this method does not update the database, changes to the association object must be done for
   * the operation to be persisted. Alternatively the method 'associate(o, a)' will call this assign(o, a)
   * and persist the changes.
   *
   * @return the 'a' parameter is returned
   */
  def assign(o: O, a: A): A

  /**
   * @param a: the association object
   *
   * Calls assign(o,a) and persists the changes the database, by inserting or updating 'a', depending
   * on if it has been persisted or not.
   *
   * @return the 'a' parameter is returned
   */
  def associate(o: O, a: A): A

  /**
   * Creates a new association object 'a' and calls assign(o,a)
   */
  def assign(o: O): A

  /**
   * Creates a new association object 'a' and calls associate(o,a)
   *
   * Note that this method will fail if the association object has NOT NULL constraint fields apart from the
   * foreign keys in the relations
   *  
   */
  def associate(o: O): A

  /**
   * Causes the deletion of the 'Association object' between this side and the other side
   * of the relation.
   * @return true if 'o' was associated (if an association object existed between 'this' and 'o') false otherwise
   */

  def dissociate(o: O): Boolean

  /**
   *  Deletes all "associations" relating this "side" to the other
   */
  def dissociateAll: Int

  /**
   * a Query returning all of this member's association entries 
   */
  def associations: Query[A]

  /**
   * @return a Query of Tuple2 containing all objects on the 'other side' along with their association object
   */
  def associationMap: Query[(O, A)]
}

/**
 * An in-memory cached view of one side of a many-to-many relationship. Maintains a
 * map of associated entities `O` to their association objects `A`. Eagerly loads all
 * associations on construction.
 *
 * Mutations (associate, dissociate, dissociateAll) update both the database and the
 * local cache. The `dissociate` method includes an assertion to detect cache/database
 * synchronization issues.
 *
 * Call `refresh` to re-synchronize the cache with the database.
 *
 * @tparam O the entity type on the "other" side of the relationship
 * @tparam A the association entity type
 */
class StatefulManyToMany[O, A](val relation: ManyToMany[O, A]) extends Iterable[O] {

  private[this] val _map = new HashMap[O, A]

  refresh

  /** Clears the local cache and reloads all associations from the database. */
  def refresh = {
    _map.clear()
    for (e <- relation.associationMap.iterator.toSeq)
      _map.put(e._1, e._2)
  }

  def iterator = _map.keysIterator

  /** Associates `o` using the provided association object `a`, persists it, and updates the local cache. */
  def associate(o: O, a: A) = {
    relation.associate(o, a)
    _map.put(o, a)
    a
  }

  /** Associates `o` with a default association object, persists it, and updates the local cache. */
  def associate(o: O): A = {
    val a = relation.associate(o)
    _map.put(o, a)
    a
  }

  /**
   * Removes the association between this side and `o` from both the database and the
   * local cache. Asserts that the database and cache are in sync.
   *
   * @return true if the association existed and was removed
   */
  def dissociate(o: O): Boolean = {
    val b1 = relation.dissociate(o)
    val b2 = _map.remove(o).isDefined
    assert(
      b1 == b2,
      "'MutableManyToMany out of sync " + o.asInstanceOf[AnyRef].getClass.getName + " with id=" +
        relation.kedL.getId(o) + (if (b1) ""
                                  else "does not") + " exist in the db, and cached collection says the opposite"
    )
    b1
  }

  /** Removes all associations from the database and clears the local cache. */
  def dissociateAll: Int = {
    val r = relation.dissociateAll
    _map.clear()
    r
  }

  /** Returns all cached association objects. */
  def associations: Iterable[A] =
    _map.values.toSeq
}

/**
 * Represents the "many" side of a one-to-many relationship as a queryable collection.
 * Extends [[Query]] so it can be used directly in Squeryl queries and iterated over.
 *
 * Provides `assign` to set the foreign key without persisting, and `associate` to
 * both set the foreign key and persist the entity.
 *
 * @tparam M the entity type on the "many" side
 */
trait OneToMany[M] extends Query[M] {

  /**
   * @param m the object on the 'many side' to be associated with this
   *
   *  Sets the foreign key of 'm' to refer to the primary key of the 'one' instance
   *
   * @return the 'm' parameter is returned
   */
  def assign(m: M): M

  /**
   * Calls 'assign(m)' and persists the changes the database, by inserting or updating 'm', depending
   * on if it has been persisted or not.
   *
   * @return the 'm' parameter is returned
   */
  def associate(m: M): M

  /** Deletes all entities on the "many" side associated with the "one" side. */
  def deleteAll: Discardable[Int]
}

/**
 * Represents the "one" side of a many-to-one relationship as a queryable reference.
 * Extends [[Query]] so it can be used directly in Squeryl queries. Iteration yields
 * at most one result (the parent entity).
 *
 * @tparam O the entity type on the "one" (parent) side
 */
trait ManyToOne[O] extends Query[O] {

  /**
   * Assigns the foreign key with the value of the 'one' primary ky
   *
   * @return the 'one' parameter is returned
   */
  def assign(one: O): O

  /** Deletes the parent entity on the "one" side. Returns true if a row was deleted. */
  def delete: Discardable[Boolean]
}
