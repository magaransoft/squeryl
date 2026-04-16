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

import dsl.*
import ast.*
import internals.*
import org.squeryl.helpers.Discardable

import reflect.ClassTag
import java.sql.SQLException
import java.io.PrintWriter
import java.util.regex.Pattern
import collection.mutable.{ArrayBuffer, HashMap, HashSet}
import org.squeryl.internals.FieldMapper

/**
 * Base class for defining a database schema in Squeryl.
 *
 * A `Schema` serves as the container for all table and relation declarations. Subclass `Schema`
 * and declare tables using the `table[T]` method (inherited from [[internals.TableDefinitionInSchema]]).
 * The schema provides DDL generation ([[create]], [[drop]], [[printDdl]]), naming conventions,
 * lifecycle callbacks, foreign key policies, and column attribute defaults.
 *
 * Example:
 * {{{
 * object MySchema extends Schema {
 *   val people = table[Person]
 *   val addresses = table[Address]
 *
 *   on(people)(p => declare(
 *     p.name is(indexed)
 *   ))
 * }
 * }}}
 *
 * @param fieldMapper the implicit field mapper used for type-to-column mappings
 */
class Schema(implicit val fieldMapper: FieldMapper) extends TableDefinitionInSchema {

  protected implicit def thisSchema: Schema = this

  /**
   * Contains all Table[_]s in this shema, and also all ManyToManyRelation[_,_,_]s (since they are also Table[_]s
   */
  private[this] val _tables = new ArrayBuffer[Table[_]]

  /**
   * Returns all tables registered in this schema, including many-to-many relation tables.
   */
  def tables: collection.Seq[Table[_]] = _tables.toSeq

  private[this] val _tableTypes = new HashMap[Class[_], Table[_]]

  private[this] val _oneToManyRelations = new ArrayBuffer[OneToManyRelation[_, _]]

  private[this] val _manyToManyRelations = new ArrayBuffer[ManyToManyRelation[_, _, _]]

  private[this] val _columnGroupAttributeAssignments = new ArrayBuffer[ColumnGroupAttributeAssignment]

  private[squeryl] val _namingScope = new HashSet[String]

  private[squeryl] def _addRelation(r: OneToManyRelation[_, _]) =
    _oneToManyRelations.append(r)

  private[squeryl] def _addRelation(r: ManyToManyRelation[_, _, _]) =
    _manyToManyRelations.append(r)

  private def _dbAdapter = Session.currentSession.databaseAdapter

  /**
   *  @returns a tuple of (Table[_], Table[_], ForeignKeyDeclaration) where
   *  ._1 is the foreign key table,
   *  ._2 is the primary key table
   *  ._3 is the ForeignKeyDeclaration between _1 and _2
   */
  private def _activeForeignKeySpecs = {
    val res = new ArrayBuffer[(Table[_], Table[_], ForeignKeyDeclaration)]

    for (r <- _oneToManyRelations if r.foreignKeyDeclaration._isActive)
      res.append((r.rightTable, r.leftTable, r.foreignKeyDeclaration))

    for (r <- _manyToManyRelations) {
      if (r.leftForeignKeyDeclaration._isActive)
        res.append((r.thisTable, r.leftTable, r.leftForeignKeyDeclaration))
      if (r.rightForeignKeyDeclaration._isActive)
        res.append((r.thisTable, r.rightTable, r.rightForeignKeyDeclaration))
    }

    res
  }

  /**
   * Finds all tables whose row type exactly matches the runtime class of the given object.
   *
   * @tparam A the row type
   * @param a an instance whose class is used to match tables
   * @return the tables whose POSO class equals `a`'s runtime class
   */
  def findTablesFor[A](a: A): Iterable[Table[A]] = {
    val c = a.asInstanceOf[AnyRef].getClass
    _tables.filter(_.posoMetaData.clasz == c).asInstanceOf[Iterable[Table[A]]]
  }

  /**
   * Finds all tables whose row type is assignable from the given class.
   *
   * Unlike [[findTablesFor]], this includes tables mapped to subclasses of `A`.
   *
   * @tparam A the base type to search for
   * @param c the class to match against (including subclasses)
   * @return all tables whose POSO class is assignable from `c`
   */
  def findAllTablesFor[A](c: Class[A]) =
    _tables.filter(t => c.isAssignableFrom(t.posoMetaData.clasz)).asInstanceOf[Iterable[Table[_]]]

  /**
   * Provides naming convention transformation utilities for converting between camelCase
   * and snake_case identifiers, commonly used when mapping Scala property names to
   * database column names.
   */
  object NamingConventionTransforms {

    @deprecated("use snakify() instead as of 0.9.5beta", "0.9.5")
    def camelCase2underScore(name: String) =
      name.toList.map(c => if (c.isUpper) "_" + c else c).mkString

    private final val FIRST_PATTERN = Pattern.compile("^([^A-Za-z_])")
    private final val SECOND_PATTERN = Pattern.compile("([A-Z]+)([A-Z][a-z])")
    private final val THIRD_PATTERN = Pattern.compile("([a-z0-9])([A-Z])")

    /**
     * Converts a camelCase or PascalCase identifier to snake_case.
     *
     * Handles consecutive uppercase letters correctly (e.g. "HTMLParser" becomes "html_parser").
     * Leading non-alphabetic/non-underscore characters are prefixed with an underscore.
     *
     * @param name the identifier to convert
     * @return the snake_case version of the name
     */
    def snakify(name: String): String =
      THIRD_PATTERN
        .matcher(SECOND_PATTERN.matcher(FIRST_PATTERN.matcher(name).replaceAll("_$1")).replaceAll("$1_$2"))
        .replaceAll("$1_$2")
        .toLowerCase
  }

  /**
   * Maps a Scala property name to a database column name.
   *
   * Override this method to apply a naming convention transformation (e.g. camelCase to
   * snake_case via [[NamingConventionTransforms.snakify]]). The default implementation
   * returns the property name unchanged.
   *
   * @param propertyName the Scala property name
   * @return the corresponding database column name
   */
  def columnNameFromPropertyName(propertyName: String) = propertyName

  /**
   * Maps a class name to a database table name.
   *
   * Override this method to apply a naming convention transformation. The default
   * implementation returns the class name unchanged.
   *
   * @param tableName the simple class name
   * @return the corresponding database table name
   */
  def tableNameFromClassName(tableName: String) = tableName

  /**
   * Returns the optional schema name (database schema/namespace prefix).
   *
   * When defined, table names are prefixed with this value (e.g. "myschema.MyTable").
   * The default is `None`, meaning no prefix is applied.
   *
   * @return the optional schema name
   */
  def name: Option[String] = None

  /**
   * Prints the schema to standard output, it is simply : schema.printDdl(println(_))
   */
  def printDdl: Unit = printDdl(println(_))

  /** Prints the DDL to the given `PrintWriter`. */
  def printDdl(pw: PrintWriter): Unit = printDdl(pw.println(_))

  /**
   * @param statementHandler is a closure that receives every declaration in the schema.
   */
  def printDdl(statementHandler: String => Unit): Unit = {

    statementHandler("-- table declarations :")

    for (t <- _tables) {
      val sw = new StatementWriter(true, _dbAdapter)
      _dbAdapter.writeCreateTable(t, sw, this)
      statementHandler(sw.statement + ";")
      _dbAdapter.postCreateTable(t, Some(statementHandler))

      val indexDecl = _indexDeclarationsFor(t)

      if (indexDecl != Nil)
        statementHandler("-- indexes on " + t.prefixedName)

      for (i <- indexDecl)
        statementHandler(i + ";")
    }

    val constraints = _foreignKeyConstraints.toList

    if (constraints != Nil)
      statementHandler("-- foreign key constraints :")

    for (fkc <- constraints)
      statementHandler(fkc + ";")

    val compositePKs = _allCompositePrimaryKeys.toList

    if (compositePKs != Nil)
      statementHandler("-- composite key indexes :")

    for (cpk <- compositePKs) {
      val createConstraintStmt = _dbAdapter.writeCompositePrimaryKeyConstraint(cpk._1, cpk._2)
      statementHandler(createConstraintStmt + ";")
    }

    val columnGroupIndexes = _writeColumnGroupAttributeAssignments.toList

    if (columnGroupIndexes != Nil)
      statementHandler("-- column group indexes :")

    for (decl <- columnGroupIndexes)
      statementHandler(decl + ";")
  }

  /**
   * This will drop all tables and related sequences in the schema... it's a
   * dangerous operation, typically this is only useful for development
   * database instances, the method is protected in order to make it a little
   * less 'accessible'
   */
  def drop: Unit = {

    if (_dbAdapter.supportsForeignKeyConstraints)
      _dropForeignKeyConstraints

    Session.currentSession.connection.createStatement
    Session.currentSession.connection

    for (t <- _tables) {
      _dbAdapter.dropTable(t)
      _dbAdapter.postDropTable(t)
    }
  }

  /**
   * Creates all tables, foreign key constraints, composite primary key constraints, and
   * column group indexes in the database.
   *
   * Executes DDL statements against the current session's connection. Tables are created first,
   * followed by foreign key constraints (if supported by the adapter), composite primary key
   * constraints, and finally column group constraints and indexes.
   */
  def create = {
    _createTables
    if (_dbAdapter.supportsForeignKeyConstraints)
      _declareForeignKeyConstraints

    _createConstraintsOfCompositePKs

    createColumnGroupConstraintsAndIndexes
  }

  private def _indexDeclarationsFor(t: Table[_]): List[String] = {
    t.posoMetaData.fieldsMetaData.flatMap { fmd =>
      _writeIndexDeclarationIfApplicable(fmd.columnAttributes.toSeq, Seq(fmd), None)
    }.toList
  }

  private def _writeColumnGroupAttributeAssignments: collection.Seq[String] =
    for (cgaa <- _columnGroupAttributeAssignments)
      yield _writeIndexDeclarationIfApplicable(cgaa.columnAttributes, cgaa.columns, cgaa.name).getOrElse(
        org.squeryl.internals.Utils
          .throwError("empty attribute list should not be possible to create with DSL (Squeryl bug).")
      )

  private def _writeIndexDeclarationIfApplicable(
    columnAttributes: collection.Seq[ColumnAttribute],
    cols: collection.Seq[FieldMetaData],
    name: Option[String]
  ): Option[String] = {

    val unique = columnAttributes.find(_.isInstanceOf[Unique])
    val indexed = columnAttributes.collectFirst { case i: Indexed => i }

    (unique, indexed) match {
      case (None, None) => None
      case (Some(_), None) => Some(_dbAdapter.writeIndexDeclaration(cols, None, name, true))
      case (None, Some(Indexed(idxName))) => Some(_dbAdapter.writeIndexDeclaration(cols, idxName, name, false))
      case (Some(_), Some(Indexed(idxName))) => Some(_dbAdapter.writeIndexDeclaration(cols, idxName, name, true))
    }
  }

  /** Creates column group constraints and indexes (composite unique, composite indexes) in the database. */
  def createColumnGroupConstraintsAndIndexes =
    for (statement <- _writeColumnGroupAttributeAssignments)
      _executeDdl(statement)

  private def _dropForeignKeyConstraints = {

    val cs = Session.currentSession
    val dba = cs.databaseAdapter

    for (fk <- _activeForeignKeySpecs) {
      cs.connection.createStatement
      dba.dropForeignKeyStatement(fk._1, dba.foreignKeyConstraintName(fk._1, fk._3.idWithinSchema), cs)
    }
  }

  private def _declareForeignKeyConstraints =
    for (fk <- _foreignKeyConstraints)
      _executeDdl(fk)

  private def _executeDdl(statement: String) = {

    val cs = Session.currentSession
    cs.log(statement)

    val s = cs.connection.createStatement
    try {
      s.execute(statement)
    } catch {
      case e: SQLException => throw SquerylSQLException("error executing " + statement + "\n" + e, e)
    } finally {
      s.close
    }
  }

  private def _foreignKeyConstraints =
    for (fk <- _activeForeignKeySpecs) yield {
      val fkDecl = fk._3

      _dbAdapter.writeForeignKeyDeclaration(
        fk._1,
        fkDecl.foreignKeyColumnName,
        fk._2,
        fkDecl.referencedPrimaryKey,
        fkDecl._referentialAction1,
        fkDecl._referentialAction2,
        fkDecl.idWithinSchema
      )
    }

  private def _createTables = {
    for (t <- _tables) {
      val sw = new StatementWriter(_dbAdapter)
      _dbAdapter.writeCreateTable(t, sw, this)
      _executeDdl(sw.statement)
      _dbAdapter.postCreateTable(t, None)
      for (indexDecl <- _indexDeclarationsFor(t))
        _executeDdl(indexDecl)
    }
  }

  private def _createConstraintsOfCompositePKs =
    for (cpk <- _allCompositePrimaryKeys) {
      val createConstraintStmt = _dbAdapter.writeCompositePrimaryKeyConstraint(cpk._1, cpk._2)
      _executeDdl(createConstraintStmt)
    }

  /**
   * returns an Iterable of (Table[_],Iterable[FieldMetaData]), the list of
   * all tables whose PK is a composite, with the columns that are part of the PK : Iterable[FieldMetaData] 
   */
  private def _allCompositePrimaryKeys = {

    val res = new ArrayBuffer[(Table[_], Iterable[FieldMetaData])]

    for (t <- _tables; ked <- t.ked) {

      Utils.mapSampleObject(
        t.asInstanceOf[Table[AnyRef]],
        (z: AnyRef) => {
          ked.asInstanceOf[KeyedEntityDef[AnyRef, AnyRef]].getId(z) match {
            case id: CompositeKey =>
              val compositeCols = id._fields
              res.append((t, compositeCols))
            case _ =>
          }
        }
      )
    }

    res
  }

  /**
   * Use this method to override the DatabaseAdapter's default column type for the given field
   * (FieldMetaData), returning None means that no override will take place.
   *
   * There are two levels at which db column type can be overridden, in order of precedence :
   *
   *   on(professors)(p => declare(
   *      s.yearlySalary is(dbType("real"))
   *    ))
   *
   *  overrides (has precedence over) :
   *
   *  MySchema extends Schema {
   *    ...
   *    override def columnTypeFor(fieldMetaData: FieldMetaData, owner: Table[_]) =
   *      if(fieldMetaData.wrappedFieldType.isInstanceOf[Int)
   *        Some("number")
   *      else
   *        None
   *  }
   *
   */
  def columnTypeFor(fieldMetaData: FieldMetaData, owner: Table[_]): Option[String] = None

  /** Returns the table name derived from the given class's simple name. */
  def tableNameFromClass(c: Class[_]): String =
    c.getSimpleName

  private[squeryl] def _addTable(t: Table[_]) =
    _tables.append(t)

  private[squeryl] def _addTableType(typeT: Class[_], t: Table[_]) =
    _tableTypes += ((typeT, t))

  /**
   * Represents a referential integrity event type (update or delete) used in foreign key
   * declarations to specify the referential action.
   *
   * @param eventName the SQL event name ("update" or "delete")
   */
  class ReferentialEvent(val eventName: String) {
    /** Creates a RESTRICT referential action for this event. */
    def restrict = new ReferentialActionImpl("restrict", this)
    /** Creates a CASCADE referential action for this event. */
    def cascade = new ReferentialActionImpl("cascade", this)
    /** Creates a NO ACTION referential action for this event. */
    def noAction = new ReferentialActionImpl("no action", this)
    /** Creates a SET NULL referential action for this event. */
    def setNull = new ReferentialActionImpl("set null", this)
  }

  /**
   * Concrete implementation of a referential action (e.g. "cascade", "restrict", "set null")
   * bound to a specific referential event.
   *
   * @param token the SQL action token (e.g. "cascade", "no action")
   * @param ev the referential event this action applies to
   */
  class ReferentialActionImpl(token: String, ev: ReferentialEvent) extends ReferentialAction {
    /** Returns the event name (e.g. "update" or "delete"). */
    def event = ev.eventName
    /** Returns the action token (e.g. "cascade", "restrict"). */
    def action = token
  }

  /** Creates a referential event for ON UPDATE foreign key actions. */
  protected def onUpdate = new ReferentialEvent("update")

  /** Creates a referential event for ON DELETE foreign key actions. */
  protected def onDelete = new ReferentialEvent("delete")

  private[this] var _fkIdGen = 1

  private[squeryl] def _createForeignKeyDeclaration(fkColName: String, pkColName: String) = {
    val fkd = new ForeignKeyDeclaration(_fkIdGen, fkColName, pkColName)
    _fkIdGen += 1
    applyDefaultForeignKeyPolicy(fkd)
    fkd
  }

  /**
   * Applies the default referential integrity policy to a newly created foreign key declaration.
   *
   * Override this method to change the default foreign key behavior for the entire schema
   * (e.g. to set cascade delete by default). The default implementation calls
   * `constrainReference()` which applies the database's default referential action.
   *
   * @param foreignKeyDeclaration the foreign key declaration to configure
   */
  def applyDefaultForeignKeyPolicy(foreignKeyDeclaration: ForeignKeyDeclaration) =
    foreignKeyDeclaration.constrainReference()

  /**
   * @return a Tuple2 with (LengthOfDecimal, Scale) that will determine the storage
   * length of the database type that map fields of type java.lang.BigDecimal
   * Can be overridden by the Column Annotation, ex.: Column(length=22, scale=20)
   * default is (20,16)
   */

  def defaultSizeOfBigDecimal = (20, 16)

  /**
   * @return the default database storage (column) length for String columns for this Schema,
   * Can be overridden by the Column Annotation ex.: Column(length=256)
   * default is 128 
   */
  def defaultLengthOfString = 128

  /**
   * protected since table declarations must only be done inside a Schema
   */
  protected def declare[B](a: BaseColumnAttributeAssignment*) = a

  /**
   * protected since table declarations must only be done inside a Schema
   */
  protected def on[A](table: Table[A])(declarations: A => Seq[BaseColumnAttributeAssignment]) = {

    if (table == null)
      org.squeryl.internals.Utils.throwError(
        "on function called with null argument in " + this.getClass.getName +
          " tables must be initialized before declarations."
      )

    val colAss: collection.Seq[BaseColumnAttributeAssignment] =
      Utils.mapSampleObject(table, declarations)

    // all fields that have a single 'is' declaration are first reset :
    for (ca <- colAss if ca.isInstanceOf[ColumnAttributeAssignment])
      ca.clearColumnAttributes

    for (ca <- colAss) ca match {
      case dva: DefaultValueAssignment => {

        dva.value match {
          case x: ConstantTypedExpression[_, _] =>
            dva.left._defaultValue = Some(x)
          case _ =>
            org.squeryl.internals.Utils.throwError(
              "error in declaration of column " + table.prefixedName + "." + dva.left.nameOfProperty + ", " +
                "only constant expressions are supported in 'defaultsTo' declaration"
            )
        }
      }
      case caa: ColumnAttributeAssignment => {

        for (ca <- caa.columnAttributes)
          (caa.left._addColumnAttribute(ca))

        // don't allow a KeyedEntity.id field to not have a uniqueness constraint :
        if (ca.isIdFieldOfKeyedEntityWithoutUniquenessConstraint)
          caa.left._addColumnAttribute(primaryKey)
      }
      case ctaa: ColumnGroupAttributeAssignment => {

        // don't allow a KeyedEntity.id field to not have a uniqueness constraint :
        if (ca.isIdFieldOfKeyedEntityWithoutUniquenessConstraint)
          ctaa.addAttribute(primaryKey)

        _addColumnGroupAttributeAssignment(ctaa)
      }

      case a: Any => org.squeryl.internals.Utils.throwError("did not match on " + a.getClass.getName)
    }

//    for(ca <- colAss.find(_.isIdFieldOfKeyedEntity))
//      assert(
//        ca.columnAttributes.exists(_.isInstanceOf[PrimaryKey]) ||
//        ca.columnAttributes.exists(_.isInstanceOf[Unique]),
//        "Column 'id' of table '" + table.name +
//        "' must have a uniqueness constraint by having the column attribute 'primaryKey' or 'unique' to honor it's KeyedEntity trait"
//      )

    // Validate that autoIncremented is not used on other fields than KeyedEntity[A].id :
    // since it is not yet unsupported :
    for (ca <- colAss) ca match {
      case cga: CompositeKeyAttributeAssignment => {}
      case caa: ColumnAttributeAssignment => {
        for (ca <- caa.columnAttributes if ca.isInstanceOf[AutoIncremented] && !(caa.left.isIdFieldOfKeyedEntity))
          org.squeryl.internals.Utils.throwError(
            "Field " + caa.left.nameOfProperty + " of table " + table.name +
              " is declared as autoIncremented, auto increment is currently only supported on KeyedEntity[A].id"
          )
      }
      case dva: Any => {}
    }
  }

  private def _addColumnGroupAttributeAssignment(cga: ColumnGroupAttributeAssignment) =
    _columnGroupAttributeAssignments.append(cga);

  /**
   * Returns the default column attributes for the `id` field of a [[KeyedEntity]].
   *
   * For `Long` and `Integer` id types, the default is `primaryKey` + `autoIncremented`.
   * For all other types, only `primaryKey` is applied. Override this method to customize
   * the default key column behavior (e.g. to use sequences instead of auto-increment).
   *
   * @param typeOfIdField the runtime class of the id field
   * @return the set of column attributes to apply
   */
  def defaultColumnAttributesForKeyedEntityId(typeOfIdField: Class[_]) =
    if (
      typeOfIdField
        .isAssignableFrom(classOf[java.lang.Long]) || typeOfIdField.isAssignableFrom(classOf[java.lang.Integer])
    )
      Set(new PrimaryKey, AutoIncremented(None))
    else
      Set(new PrimaryKey)

  /** Column attribute declaring a unique constraint. */
  protected def unique = Unique()

  /** Column attribute declaring the column as a primary key. */
  protected def primaryKey = PrimaryKey()

  /** Column attribute declaring the column as auto-incremented with a database-generated sequence. */
  protected def autoIncremented = AutoIncremented(None)

  /** Column attribute declaring the column as auto-incremented with the specified sequence name. */
  protected def autoIncremented(sequenceName: String) = AutoIncremented(Some(sequenceName))

  /** Column attribute declaring the column as indexed. */
  protected def indexed = Indexed(None)

  /** Column attribute declaring the column as indexed with the specified index name. */
  protected def indexed(indexName: String) = Indexed(Some(indexName))

  /** Column attribute overriding the default database type with an explicit declaration. */
  protected def dbType(declaration: String) = DBType(declaration)

  /** Column attribute excluding the column from INSERT statements. */
  protected def uninsertable = Uninsertable()

  /** Column attribute excluding the column from UPDATE statements. */
  protected def unupdatable = Unupdatable()

  /** Column attribute overriding the column name derived from the property name. */
  protected def named(name: String) = Named(name)

  /** Column attribute marking the field as transient (not persisted to the database). */
  protected def transient = IsTransient()

  /**
   * A declaration of a group of columns used to apply multi-column attributes such as
   * composite unique constraints or composite indexes.
   *
   * @param cols the field metadata for the columns in the group
   */
  class ColGroupDeclaration(cols: collection.Seq[FieldMetaData]) {

    /** Assigns the given multi-column attributes (e.g. composite unique) to this column group. */
    def are(columnAttributes: AttributeValidOnMultipleColumn*) =
      new ColumnGroupAttributeAssignment(cols, columnAttributes)
  }

  /** Creates a column group declaration from the given typed expressions, for use with multi-column constraints. */
  def columns(fieldList: TypedExpression[_, _]*) = new ColGroupDeclaration(fieldList.map(_._fieldMetaData))

  // POSO Life Cycle Callbacks :

  /**
   * Override this method to declare lifecycle callbacks for tables in this schema.
   *
   * Return a sequence of lifecycle event declarations built using the `beforeInsert`,
   * `afterInsert`, `beforeUpdate`, `afterUpdate`, `beforeDelete`, `afterDelete`,
   * `afterSelect`, and `factoryFor` methods.
   *
   * Example:
   * {{{
   * override def callbacks = Seq(
   *   beforeInsert(myTable) call { row => row.createdAt = new Timestamp(System.currentTimeMillis) },
   *   afterSelect(myTable) call { row => row.initTransientFields() }
   * )
   * }}}
   *
   * @return the lifecycle event declarations (default is Nil)
   */
  def callbacks: collection.Seq[LifecycleEvent] = Nil

////2.9.x approach for LyfeCycle events :
//  def delayedInit(body: => Unit) = {
//
//    body
//
//    (for(cb <- callbacks; t <- cb.target) yield (t, cb))
//    .groupBy(_._1)
//    .mapValues(_.map(_._2))
//    .foreach(
//     (t:Tuple2[View[_],Seq[LifecycleEvent]]) => {
//       t._1._callbacks = new LifecycleEventInvoker(t._2, t._1)
//     }
//    )
//  }

////2.8.x approach for LyfeCycle events :
  private[squeryl] lazy val _callbacks: Map[View[_], LifecycleEventInvoker] = {
    val m =
      (for (cb <- callbacks; t <- cb.target) yield (t, cb))
        .groupBy(_._1)
        .mapValues(_.map(_._2))
        .map((t: Tuple2[View[_], collection.Seq[LifecycleEvent]]) => {
          (t._1, new LifecycleEventInvoker(t._2, t._1)): (View[_], LifecycleEventInvoker)
        })
        .toMap
    m
  }

  import internals.PosoLifecycleEvent._

  /**
   * Creates a lifecycle event precursor for the beforeInsert event on a specific table.
   * Use in the `callbacks` override to register a callback invoked before each row insertion.
   *
   * @tparam A the row type
   * @param t the target table
   */
  protected def beforeInsert[A](t: Table[A]) =
    new LifecycleEventPercursorTable[A](t, BeforeInsert)

  /**
   * Creates a lifecycle event precursor for the beforeInsert event matching all tables of type `A`.
   *
   * @tparam A the row type (resolved via ClassTag)
   */
  protected def beforeInsert[A]()(implicit m: ClassTag[A]) =
    new LifecycleEventPercursorClass[A](m.runtimeClass, this, BeforeInsert)

  /**
   * Creates a lifecycle event precursor for the beforeUpdate event on a specific table.
   *
   * @tparam A the row type
   * @param t the target table
   */
  protected def beforeUpdate[A](t: Table[A]) =
    new LifecycleEventPercursorTable[A](t, BeforeUpdate)

  /**
   * Creates a lifecycle event precursor for the beforeUpdate event matching all tables of type `A`.
   *
   * @tparam A the row type (resolved via ClassTag)
   */
  protected def beforeUpdate[A]()(implicit m: ClassTag[A]) =
    new LifecycleEventPercursorClass[A](m.runtimeClass, this, BeforeUpdate)

  /**
   * Creates a lifecycle event precursor for the beforeDelete event on a specific table.
   * Requires an implicit [[KeyedEntityDef]] because delete operates by primary key.
   *
   * @tparam A the row type
   * @param t the target table
   */
  protected def beforeDelete[A](t: Table[A])(implicit ev: KeyedEntityDef[A, _]) =
    new LifecycleEventPercursorTable[A](t, BeforeDelete)

  /**
   * Creates a lifecycle event precursor for the beforeDelete event matching all tables of type `A`.
   *
   * @tparam K the key type
   * @tparam A the row type (resolved via ClassTag)
   */
  protected def beforeDelete[K, A]()(implicit m: ClassTag[A], ked: KeyedEntityDef[A, K]) =
    new LifecycleEventPercursorClass[A](m.runtimeClass, this, BeforeDelete)

  /**
   * Creates a lifecycle event precursor for the afterSelect event on a specific table.
   * The callback is invoked after each row is materialized from a result set.
   *
   * @tparam A the row type
   * @param t the target table
   */
  protected def afterSelect[A](t: Table[A]) =
    new LifecycleEventPercursorTable[A](t, AfterSelect)

  /**
   * Creates a lifecycle event precursor for the afterSelect event matching all tables of type `A`.
   *
   * @tparam A the row type (resolved via ClassTag)
   */
  protected def afterSelect[A]()(implicit m: ClassTag[A]) =
    new LifecycleEventPercursorClass[A](m.runtimeClass, this, AfterSelect)

  /**
   * Creates a lifecycle event precursor for the afterInsert event on a specific table.
   *
   * @tparam A the row type
   * @param t the target table
   */
  protected def afterInsert[A](t: Table[A]) =
    new LifecycleEventPercursorTable[A](t, AfterInsert)

  /**
   * Creates a lifecycle event precursor for the afterInsert event matching all tables of type `A`.
   *
   * @tparam A the row type (resolved via ClassTag)
   */
  protected def afterInsert[A]()(implicit m: ClassTag[A]) =
    new LifecycleEventPercursorClass[A](m.runtimeClass, this, AfterInsert)

  /**
   * Creates a lifecycle event precursor for the afterUpdate event on a specific table.
   *
   * @tparam A the row type
   * @param t the target table
   */
  protected def afterUpdate[A](t: Table[A]) =
    new LifecycleEventPercursorTable[A](t, AfterUpdate)

  /**
   * Creates a lifecycle event precursor for the afterUpdate event matching all tables of type `A`.
   *
   * @tparam A the row type (resolved via ClassTag)
   */
  protected def afterUpdate[A]()(implicit m: ClassTag[A]) =
    new LifecycleEventPercursorClass[A](m.runtimeClass, this, AfterUpdate)

  /**
   * Creates a lifecycle event precursor for the afterDelete event on a specific table.
   *
   * @tparam A the row type
   * @param t the target table
   */
  protected def afterDelete[A](t: Table[A]) =
    new LifecycleEventPercursorTable[A](t, AfterDelete)

  /**
   * Creates a lifecycle event precursor for the afterDelete event matching all tables of type `A`.
   *
   * @tparam A the row type (resolved via ClassTag)
   */
  protected def afterDelete[A]()(implicit m: ClassTag[A]) =
    new LifecycleEventPercursorClass[A](m.runtimeClass, this, AfterDelete)

  /**
   * Declares a custom factory for creating instances of a table's row type.
   *
   * Use this to override the default reflective instantiation when row objects require
   * special construction (e.g. non-default constructors, dependency injection).
   * Used within the `callbacks` override.
   *
   * @tparam A the row type
   * @param table the target table
   * @return a factory precursor that accepts a creation function via `is`
   */
  protected def factoryFor[A](table: Table[A]) =
    new PosoFactoryPercursorTable[A](table)

  /**
   * Creates a ActiveRecord instance for the given the object. That allows the user
   * to save the given object using the Active Record pattern.
   *
   * @return a instance of ActiveRecord associated to the given object.
   */
  implicit def anyRef2ActiveTransaction[A](a: A)(implicit queryDsl: QueryDsl, m: ClassTag[A]): ActiveRecord[A] =
    new ActiveRecord(a, queryDsl, m)

  /**
   * Active Record pattern implementation. Enables the user to insert an object in its
   * existent table with a convenient {{{save}}} method.
   */
  class ActiveRecord[A](a: A, queryDsl: QueryDsl, m: ClassTag[A]) {

    private def _performAction(action: (Table[A]) => Unit) =
      _tableTypes get (m.runtimeClass) map { (table: Table[_]) =>
        queryDsl inTransaction (action(table.asInstanceOf[Table[A]]))
      }

    /**
     * Same as {{{table.insert(a)}}}
     */
    def save: Discardable[Option[Unit]] =
      _performAction(_.insert(a))

    /**
     * Same as {{{table.update(a)}}}
     */
    def update(implicit ked: KeyedEntityDef[A, _]): Discardable[Option[Unit]] =
      _performAction(_.update(a))

  }

}
