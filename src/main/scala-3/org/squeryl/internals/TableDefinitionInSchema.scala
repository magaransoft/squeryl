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

import org.squeryl._
import reflect.ClassTag

/** Scala 3 version of the table declaration DSL mixed into [[org.squeryl.Schema]].
  *
  * This trait provides the `table[T](...)` family of methods that users call inside a `Schema`
  * to register entity types as database tables. Each overload comes in two flavors:
  *
  *  - An '''`inline`''' variant that uses the [[TypeInfo.fieldsInfo]] compile-time macro to
  *    automatically extract `Option`-wrapped primitive field metadata from `T`. This is the
  *    version users normally call.
  *  - A non-inline variant that accepts a pre-computed `optionalFieldsInfo` map. This is the
  *    delegation target of the inline variant and can also be called directly when field metadata
  *    is obtained through other means.
  *
  * The Scala 3-specific inline/macro approach replaces the Scala 2 implementation, which relies
  * on `ScalaSig` to inspect `Option` type parameters at runtime. Since Scala 3 no longer emits
  * `ScalaSig`, the macro extracts this information at compile time instead.
  *
  * This trait requires the `Schema` self-type and delegates to `Schema._addTable` and
  * `Schema._addTableType` for registration.
  */
trait TableDefinitionInSchema {
  self: Schema =>

  /** Declares a table using a name derived from the class name of `T`, with field metadata
    * extracted at compile time via the [[TypeInfo.fieldsInfo]] macro.
    *
    * @tparam T the entity type mapped to the table
    * @param manifestT runtime class tag for `T`, used to derive the table name
    * @param ked optional keyed entity definition for `T`
    * @return the newly created and registered [[Table]] instance
    */
  protected inline def table[T]()(implicit manifestT: ClassTag[T], ked: OptionalKeyedEntityDef[T, _]): Table[T] =
    val optionalFieldsInfo = org.squeryl.internals.TypeInfo.fieldsInfo[T]
    table(tableNameFromClass(manifestT.runtimeClass))(optionalFieldsInfo)(manifestT, ked)

  /** Declares a table using a name derived from the class name of `T`, with explicitly provided
    * field metadata for `Option`-wrapped primitive fields.
    *
    * @tparam T the entity type mapped to the table
    * @param optionalFieldsInfo map from field name to the boxed `Class` of the primitive type
    *                           inside each `Option` field (e.g., `"age" -> classOf[Int]`)
    * @param manifestT runtime class tag for `T`
    * @param ked optional keyed entity definition for `T`
    * @return the newly created and registered [[Table]] instance
    */
  protected def table[T](
    optionalFieldsInfo: Map[String, Class[_]]
  )(implicit manifestT: ClassTag[T], ked: OptionalKeyedEntityDef[T, _]): Table[T] =
    table(tableNameFromClass(manifestT.runtimeClass))(optionalFieldsInfo)(manifestT, ked)

  /** Declares a table with an explicit name and column prefix, with field metadata extracted
    * at compile time via the [[TypeInfo.fieldsInfo]] macro.
    *
    * @tparam T the entity type mapped to the table
    * @param name the SQL table name
    * @param prefix a prefix prepended to all column names in this table
    * @param manifestT runtime class tag for `T`
    * @param ked optional keyed entity definition for `T`
    * @return the newly created and registered [[Table]] instance
    */
  protected inline def table[T](name: String, prefix: String)(implicit
    manifestT: ClassTag[T],
    ked: OptionalKeyedEntityDef[T, _]
  ): Table[T] = {
    val optionalFieldsInfo = org.squeryl.internals.TypeInfo.fieldsInfo[T]
    table(name, prefix)(optionalFieldsInfo)(manifestT, ked)
  }

  /** Declares a table with an explicit name and column prefix, with explicitly provided
    * field metadata for `Option`-wrapped primitive fields.
    *
    * @tparam T the entity type mapped to the table
    * @param name the SQL table name
    * @param prefix a prefix prepended to all column names in this table
    * @param optionalFieldsInfo map from field name to the boxed `Class` of the primitive type
    *                           inside each `Option` field
    * @param manifestT runtime class tag for `T`
    * @param ked optional keyed entity definition for `T`
    * @return the newly created and registered [[Table]] instance
    */
  protected def table[T](name: String, prefix: String)(
    optionalFieldsInfo: Map[String, Class[_]]
  )(implicit manifestT: ClassTag[T], ked: OptionalKeyedEntityDef[T, _]): Table[T] = {
    val typeT = manifestT.runtimeClass.asInstanceOf[Class[T]]
    val t = new Table[T](name, typeT, this, Some(prefix), ked.keyedEntityDef, None)
    _addTable(t)
    _addTableType(typeT, t)
    t
  }

  /** Declares a table with an explicit name, with field metadata extracted at compile time
    * via the [[TypeInfo.fieldsInfo]] macro.
    *
    * @tparam T the entity type mapped to the table
    * @param name the SQL table name
    * @param manifestT runtime class tag for `T`
    * @param ked optional keyed entity definition for `T`
    * @return the newly created and registered [[Table]] instance
    */
  protected inline def table[T](
    name: String
  )(implicit manifestT: ClassTag[T], ked: OptionalKeyedEntityDef[T, _]): Table[T] =
    val optionalFieldsInfo = org.squeryl.internals.TypeInfo.fieldsInfo[T]
    table(name)(optionalFieldsInfo)(manifestT, ked)

  /** Declares a table with an explicit name, with explicitly provided field metadata for
    * `Option`-wrapped primitive fields.
    *
    * This is the terminal overload that actually constructs the [[Table]] instance and registers
    * it with the schema. The `optionalFieldsInfo` map is passed to the `Table` constructor so
    * that `FieldMetaData` can correctly resolve the JDBC type of `Option`-wrapped primitive fields
    * despite type erasure.
    *
    * @tparam T the entity type mapped to the table
    * @param name the SQL table name
    * @param optionalFieldsInfo map from field name to the boxed `Class` of the primitive type
    *                           inside each `Option` field
    * @param manifestT runtime class tag for `T`
    * @param ked optional keyed entity definition for `T`
    * @return the newly created and registered [[Table]] instance
    */
  protected def table[T](name: String)(
    optionalFieldsInfo: Map[String, Class[_]]
  )(implicit manifestT: ClassTag[T], ked: OptionalKeyedEntityDef[T, _]): Table[T] = {
    val typeT = manifestT.runtimeClass.asInstanceOf[Class[T]]
    val t = new Table[T](name, typeT, this, None, ked.keyedEntityDef, Some(optionalFieldsInfo))
    _addTable(t)
    _addTableType(typeT, t)
    t
  }

}
