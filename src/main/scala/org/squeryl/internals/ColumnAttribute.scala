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
package org.squeryl.internals

/**
 * Base trait for all column-level attributes that can be applied to table columns
 * via the `is(...)` method in schema definitions. Column attributes control DDL
 * generation (e.g., uniqueness constraints, auto-increment, indexing) and query
 * behavior (e.g., transient fields, uninsertable/unupdatable columns).
 */
trait ColumnAttribute

/**
 * Marker trait for attributes that can be applied to composite (multiple) column
 * declarations, such as composite unique constraints or composite indexes.
 */
trait MultipleColumnAttribute

/** Marker trait for attributes valid on composite column declarations in schema definitions. */
trait AttributeValidOnMultipleColumn extends ColumnAttribute

/** Marker trait for attributes that can be applied to numerical columns. */
trait AttributeValidOnNumericalColumn extends ColumnAttribute

/** Marker trait for attributes that can be applied to non-numerical columns (e.g., String, Date). */
trait AttributeValidOnNonNumericalColumn extends ColumnAttribute

/**
 * Column attribute that adds a UNIQUE constraint. Can be applied to individual columns
 * or to composite column declarations for multi-column unique constraints.
 * Generates `UNIQUE` in DDL.
 */
case class Unique()
    extends ColumnAttribute
    with MultipleColumnAttribute
    with AttributeValidOnNonNumericalColumn
    with AttributeValidOnNumericalColumn
    with AttributeValidOnMultipleColumn

/**
 * Column attribute that marks a numerical column as auto-incremented. Optionally
 * specifies the name of the database sequence to use. Equality is based solely on
 * the class type (ignoring the sequence name) so that multiple `AutoIncremented`
 * attributes are considered duplicates.
 *
 * @param nameOfSequence optional name of the database sequence backing the auto-increment
 */
case class AutoIncremented(var nameOfSequence: Option[String])
    extends ColumnAttribute
    with AttributeValidOnNumericalColumn {

  override def hashCode = this.getClass.hashCode

  override def equals(any: Any) =
    any.isInstanceOf[AutoIncremented]
}

/**
 * Column attribute that creates an index on the column. Can be applied to individual
 * columns or composite column declarations. Optionally specifies the index name.
 *
 * @param nameOfIndex optional name for the database index
 */
case class Indexed(nameOfIndex: Option[String])
    extends ColumnAttribute
    with MultipleColumnAttribute
    with AttributeValidOnNonNumericalColumn
    with AttributeValidOnNumericalColumn
    with AttributeValidOnMultipleColumn

/**
 * Column attribute that designates a column as a primary key. Can be applied to
 * individual columns or composite column declarations. Generates `PRIMARY KEY` in DDL.
 */
case class PrimaryKey()
    extends ColumnAttribute
    with AttributeValidOnNonNumericalColumn
    with AttributeValidOnNumericalColumn
    with AttributeValidOnMultipleColumn

/**
 * Column attribute that overrides the default SQL type for a column with an explicit
 * type declaration string. For example, `DBType("varchar(500)")` or `DBType("numeric(10,2)")`.
 *
 * When `explicit` is true, the database adapter will cast values to this type in
 * generated SQL statements.
 *
 * @param declaration the SQL type declaration string
 * @param explicit    if true, values are explicitly cast to this type in SQL
 */
case class DBType(declaration: String, explicit: Boolean = false)
    extends ColumnAttribute
    with AttributeValidOnNonNumericalColumn
    with AttributeValidOnNumericalColumn {

  /** Returns a copy of this attribute with explicit casting enabled. */
  def explicitCast = copy(explicit = true)
}

/**
 * Flag column as not accepting values on INSERT
 */
case class Uninsertable()
    extends ColumnAttribute
    with AttributeValidOnNumericalColumn
    with AttributeValidOnNonNumericalColumn

/**
 * Flag column as not accepting values on UPDATE
 */
case class Unupdatable()
    extends ColumnAttribute
    with AttributeValidOnNumericalColumn
    with AttributeValidOnNonNumericalColumn

/**
 * Column attribute that overrides the default column name (derived from the Scala field name)
 * with a custom name. Useful when the database column name differs from the Scala property name.
 *
 * @param name the custom column name to use in DDL and SQL generation
 */
case class Named(name: String)
    extends ColumnAttribute
    with AttributeValidOnNumericalColumn
    with AttributeValidOnNonNumericalColumn

/**
 * Column attribute that marks a field as transient, meaning it is excluded from all
 * database operations (SELECT, INSERT, UPDATE) and DDL generation. The field exists
 * only in the Scala object and has no corresponding database column.
 */
case class IsTransient()
    extends ColumnAttribute
    with AttributeValidOnNumericalColumn
    with AttributeValidOnNonNumericalColumn
