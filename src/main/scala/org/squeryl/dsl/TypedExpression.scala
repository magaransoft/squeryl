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

import org.squeryl.dsl.ast._
import org.squeryl.internals._
import org.squeryl.Session
import org.squeryl.Schema
import org.squeryl.Query
import java.sql.ResultSet

/**
 * Marker trait for all numeric type tags. Serves as the upper bound for arithmetic
 * operations (plus, minus, times, div) in the type-safe expression system.
 */
sealed trait TNumeric

/**
 * Type tag for `Option[BigDecimal]` columns. Extends [[TNumeric]] to participate in
 * arithmetic operations. All optional numeric type tags form a subtype chain that
 * mirrors the numeric widening rules (e.g., `TOptionInt` extends `TOptionLong`).
 */
sealed trait TOptionBigDecimal extends TNumeric

/**
 * Type tag for non-optional `BigDecimal` columns. Extends both [[TOptionBigDecimal]]
 * (allowing it to be compared with optional BigDecimal expressions) and [[TNonOption]]
 * (marking it as a non-nullable type).
 */
sealed trait TBigDecimal extends TOptionBigDecimal with TNonOption

/** Type tag for `Option[Double]` columns. Widens to [[TOptionBigDecimal]]. */
sealed trait TOptionDouble extends TOptionBigDecimal

/** Type tag for non-optional `Double` columns. */
sealed trait TDouble extends TOptionDouble with TBigDecimal with TNonOption

/** Type tag for `Option[Long]` columns. Widens to [[TOptionDouble]]. */
sealed trait TOptionLong extends TOptionDouble

/** Type tag for non-optional `Long` columns. */
sealed trait TLong extends TOptionLong with TDouble with TNonOption

/** Type tag for `Option[Float]` columns. Widens to [[TOptionDouble]]. */
sealed trait TOptionFloat extends TOptionDouble

/** Type tag for non-optional `Float` columns. */
sealed trait TFloat extends TOptionFloat with TDouble with TNonOption

/** Type tag for `Option[Int]` columns. Widens to both [[TOptionLong]] and [[TOptionFloat]]. */
sealed trait TOptionInt extends TOptionLong with TOptionFloat

/** Type tag for non-optional `Int` columns. */
sealed trait TInt extends TOptionInt with TLong with TNonOption with TFloat

/** Type tag for `Option[Byte]` columns. Widens to [[TOptionInt]]. */
sealed trait TOptionByte extends TOptionInt

/** Type tag for non-optional `Byte` columns. */
sealed trait TByte extends TOptionByte with TInt with TNonOption

/**
 * Universal option type tag. Extends every `TOption*` tag, making it the bottom type
 * for all optional type tags. Used as the type parameter for SQL `NULL` literals
 * and expressions that can match any optional column type.
 */
sealed trait TOption
    extends TOptionByte
    with TOptionInt
    with TOptionFloat
    with TOptionLong
    with TOptionDouble
    with TOptionBigDecimal
    with TOptionDate
    with TOptionLocalDate
    with TOptionLocalTime
    with TOptionString
    with TOptionTimestamp
    with TOptionLocalDateTime
    with TOptionOffsetTime
    with TOptionInstant
    with TOptionOffsetDateTime

/**
 * Bottom type for all non-optional numeric type tags. Extends every concrete numeric tag,
 * making it usable where any numeric type is expected (e.g., for literal zero).
 */
sealed trait TNumericLowerTypeBound extends TByte with TInt with TFloat with TLong with TDouble with TBigDecimal

/**
 * Marker trait for non-optional (non-nullable) type tags. All concrete non-option type
 * tags (e.g., [[TInt]], [[TString]], [[TLocalDate]]) mix in this trait, allowing the
 * type system to distinguish nullable from non-nullable expressions at compile time.
 */
sealed trait TNonOption

/**
 * Bottom type for all optional type tags. Extends every `TOption*` variant, making it
 * the lower bound in the type tag hierarchy for optional expressions. Used to type
 * expressions that can match any optional column type.
 */
sealed trait TOptionLowerBound
    extends TOptionByte
    with TOptionInt
    with TOptionFloat
    with TOptionLong
    with TOptionDouble
    with TOptionBigDecimal
    with TOptionDate
    with TOptionString
    with TOptionTimestamp
    with TOptionLocalDate
    with TOptionLocalTime
    with TOptionLocalDateTime
    with TOptionOffsetTime
    with TOptionInstant
    with TOptionOffsetDateTime

/** Type tag for non-optional enumeration value columns, parameterized by the enum type `A`. */
sealed trait TEnumValue[A]

/** Type tag for optional enumeration value columns, parameterized by the enum type `A`. */
sealed trait TOptionEnumValue[A] extends TEnumValue[A]

/** Type tag for non-optional `String` columns. */
sealed trait TString extends TOptionString with TNonOption

/** Type tag for non-optional `java.util.Date` columns. */
sealed trait TDate extends TOptionDate with TNonOption

/** Type tag for non-optional `java.time.LocalDate` columns. */
sealed trait TLocalDate extends TOptionLocalDate with TNonOption

/** Type tag for non-optional `java.time.LocalTime` columns. */
sealed trait TLocalTime extends TOptionLocalTime with TNonOption

/** Type tag for non-optional `java.sql.Timestamp` columns. */
sealed trait TTimestamp extends TOptionTimestamp with TNonOption

/** Type tag for non-optional `java.time.LocalDateTime` columns. */
sealed trait TLocalDateTime extends TOptionLocalDateTime with TNonOption

/** Type tag for non-optional `java.time.OffsetTime` columns. */
sealed trait TOffsetTime extends TOptionOffsetTime with TNonOption

/** Type tag for non-optional `java.time.Instant` columns. */
sealed trait TInstant extends TOptionInstant with TNonOption

/** Type tag for non-optional `java.time.OffsetDateTime` columns. */
sealed trait TOffsetDateTime extends TOptionOffsetDateTime with TNonOption

/** Type tag for non-optional `Array[Byte]` columns. */
sealed trait TByteArray extends TOptionByteArray with TNonOption

/** Type tag for non-optional `Array[Int]` columns. */
sealed trait TIntArray extends TOptionIntArray with TNonOption

/** Type tag for non-optional `Array[Long]` columns. */
sealed trait TLongArray extends TOptionLongArray with TNonOption

/** Type tag for non-optional `Array[Double]` columns. */
sealed trait TDoubleArray extends TOptionDoubleArray with TNonOption

/** Type tag for non-optional `Array[String]` columns. */
sealed trait TStringArray extends TOptionStringArray with TNonOption

/** Type tag for `Option[String]` columns. */
sealed trait TOptionString

/** Type tag for `Option[java.util.Date]` columns. */
sealed trait TOptionDate

/** Type tag for `Option[java.time.LocalDate]` columns. */
sealed trait TOptionLocalDate

/** Type tag for `Option[java.time.LocalTime]` columns. */
sealed trait TOptionLocalTime

/** Type tag for `Option[java.sql.Timestamp]` columns. */
sealed trait TOptionTimestamp

/** Type tag for `Option[java.time.LocalDateTime]` columns. */
sealed trait TOptionLocalDateTime

/** Type tag for `Option[java.time.OffsetTime]` columns. */
sealed trait TOptionOffsetTime

/** Type tag for `Option[java.time.Instant]` columns. */
sealed trait TOptionInstant

/** Type tag for `Option[java.time.OffsetDateTime]` columns. */
sealed trait TOptionOffsetDateTime

/** Type tag for `Option[Array[Byte]]` columns. */
sealed trait TOptionByteArray

/** Type tag for `Option[Array[Int]]` columns. */
sealed trait TOptionIntArray

/** Type tag for `Option[Array[Long]]` columns. */
sealed trait TOptionLongArray

/** Type tag for `Option[Array[Double]]` columns. */
sealed trait TOptionDoubleArray

/** Type tag for `Option[Array[String]]` columns. */
sealed trait TOptionStringArray

/** Type tag for non-optional `Boolean` columns. */
sealed trait TBoolean extends TOptionBoolean with TNonOption

/** Type tag for `Option[Boolean]` columns. */
sealed trait TOptionBoolean

/** Type tag for non-optional `java.util.UUID` columns. */
sealed trait TUUID extends TOptionUUID with TNonOption

/** Type tag for `Option[java.util.UUID]` columns. */
sealed trait TOptionUUID

/**
 * Type class witness that two type tags `A1` and `A2` are compatible for comparison
 * operations such as `===`, `<>`, `>`, `<`, `>=`, `<=`, `between`, `in`, and `notIn`.
 *
 * Implicit instances of `CanCompare` are provided by the DSL for type-compatible pairs.
 * For example, `TInt` can be compared with `TLong` because `TInt` extends `TOptionLong`.
 * Attempting to compare incompatible types (e.g., `TString` with `TInt`) results in a
 * compile-time error with the annotated message.
 *
 * @tparam A1 the type tag of the left-hand side expression
 * @tparam A2 the type tag of the right-hand side expression
 */
@scala.annotation.implicitNotFound(
  "The left side of the comparison (===, <>, between, ...) is not compatible with the right side."
)
sealed class CanCompare[-A1, -A2]

/**
 * Core trait for all type-safe SQL expressions in Squeryl's DSL. Every column reference,
 * constant, computation, and aggregate function is represented as a `TypedExpression`.
 *
 * The two type parameters encode both the Scala value type and the SQL type tag:
 *  - `A1`: the Scala type that this expression evaluates to (e.g., `Int`, `Option[String]`)
 *  - `T1`: the type tag from the sealed trait hierarchy (e.g., [[TInt]], [[TOptionString]])
 *    that governs which operations and comparisons are allowed at compile time
 *
 * Provides arithmetic operators (`+`, `-`, `*`, `/`), comparison operators (`===`, `<>`,
 * `>`, `<`, `>=`, `<=`), string operators (`like`, `ilike`, `||`), null checks (`isNull`,
 * `isNotNull`), set membership (`in`, `notIn`), range checks (`between`), regex matching,
 * and assignment operators (`:=`, `defaultsTo`).
 *
 * Arithmetic operations use implicit [[TypedExpressionFactory]] instances to determine the
 * result type via numeric widening rules. Division additionally uses [[Floatifier]] to
 * promote integral types to floating-point results.
 *
 * @tparam A1 the Scala type this expression yields
 * @tparam T1 the type tag constraining allowed operations
 */
trait TypedExpression[A1, T1] extends ExpressionNode {
  outer =>

  def plus[T3 >: T1 <: TNumeric, T2 <: T3, A2, A3](e: TypedExpression[A2, T2])(implicit
    f: TypedExpressionFactory[A3, T3]
  ): TypedExpression[A3, T3] = f.convert(new BinaryOperatorNode(this, e, "+"))

  def times[T3 >: T1 <: TNumeric, T2 <: T3, A2, A3](e: TypedExpression[A2, T2])(implicit
    f: TypedExpressionFactory[A3, T3]
  ): TypedExpression[A3, T3] = f.convert(new BinaryOperatorNode(this, e, "*"))

  def minus[T3 >: T1 <: TNumeric, T2 <: T3, A2, A3](e: TypedExpression[A2, T2])(implicit
    f: TypedExpressionFactory[A3, T3]
  ): TypedExpression[A3, T3] = f.convert(new BinaryOperatorNode(this, e, "-"))

  def div[T3 >: T1 <: TNumeric, T2 <: T3, A2, A3, A4, T4](
    e: TypedExpression[A2, T2]
  )(implicit f: TypedExpressionFactory[A3, T3], tf: Floatifier[T3, A4, T4]): TypedExpression[A4, T4] =
    tf.floatify(new BinaryOperatorNode(this, e, "/"))

  def +[T3 >: T1 <: TNumeric, T2 <: T3, A2, A3](e: TypedExpression[A2, T2])(implicit
    f: TypedExpressionFactory[A3, T3]
  ): TypedExpression[A3, T3] = plus(e)

  def *[T3 >: T1 <: TNumeric, T2 <: T3, A2, A3](e: TypedExpression[A2, T2])(implicit
    f: TypedExpressionFactory[A3, T3]
  ): TypedExpression[A3, T3] = times(e)

  def -[T3 >: T1 <: TNumeric, T2 <: T3, A2, A3](e: TypedExpression[A2, T2])(implicit
    f: TypedExpressionFactory[A3, T3]
  ): TypedExpression[A3, T3] = minus(e)

  def /[T3 >: T1 <: TNumeric, T2 <: T3, A2, A3, A4, T4](
    e: TypedExpression[A2, T2]
  )(implicit f: TypedExpressionFactory[A3, T3], tf: Floatifier[T3, A4, T4]): TypedExpression[A4, T4] =
    tf.floatify(new BinaryOperatorNode(this, e, "/"))

  def ===[A2, T2](b: TypedExpression[A2, T2])(implicit ev: CanCompare[T1, T2]) = new EqualityExpression(this, b)
  def <>[A2, T2](b: TypedExpression[A2, T2])(implicit ev: CanCompare[T1, T2]) =
    new BinaryOperatorNodeLogicalBoolean(this, b, "<>")

  def isDistinctFrom[A2, T2](b: TypedExpression[A2, T2])(implicit ev: CanCompare[T1, T2]) =
    new BinaryOperatorNodeLogicalBoolean(this, b, "IS DISTINCT FROM")
  def isNotDistinctFrom[A2, T2](b: TypedExpression[A2, T2])(implicit ev: CanCompare[T1, T2]) =
    new BinaryOperatorNodeLogicalBoolean(this, b, "IS NOT DISTINCT FROM")

  def ===[A2, T2](q: Query[Measures[A2]])(implicit tef: TypedExpressionFactory[A2, T2], ev: CanCompare[T1, T2]) =
    new BinaryOperatorNodeLogicalBoolean(this, q.copy(false, Nil).ast, "=")

  def <>[A2, T2](q: Query[Measures[A2]])(implicit tef: TypedExpressionFactory[A2, T2], ev: CanCompare[T1, T2]) =
    new BinaryOperatorNodeLogicalBoolean(this, q.copy(false, Nil).ast, "<>")

  def gt[A2, T2](b: TypedExpression[A2, T2])(implicit ev: CanCompare[T1, T2]) =
    new BinaryOperatorNodeLogicalBoolean(this, b, ">")
  def lt[A2, T2](b: TypedExpression[A2, T2])(implicit ev: CanCompare[T1, T2]) =
    new BinaryOperatorNodeLogicalBoolean(this, b, "<")
  def gte[A2, T2](b: TypedExpression[A2, T2])(implicit ev: CanCompare[T1, T2]) =
    new BinaryOperatorNodeLogicalBoolean(this, b, ">=")
  def lte[A2, T2](b: TypedExpression[A2, T2])(implicit ev: CanCompare[T1, T2]) =
    new BinaryOperatorNodeLogicalBoolean(this, b, "<=")

  def gt[A2, T2](q: Query[A2])(implicit cc: CanCompare[T1, T2]): LogicalBoolean =
    new BinaryOperatorNodeLogicalBoolean(this, q.copy(false, Nil).ast, ">")
  def gte[A2, T2](q: Query[A2])(implicit cc: CanCompare[T1, T2]): LogicalBoolean =
    new BinaryOperatorNodeLogicalBoolean(this, q.copy(false, Nil).ast, ">=")
  def lt[A2, T2](q: Query[A2])(implicit cc: CanCompare[T1, T2]): LogicalBoolean =
    new BinaryOperatorNodeLogicalBoolean(this, q.copy(false, Nil).ast, "<")
  def lte[A2, T2](q: Query[A2])(implicit cc: CanCompare[T1, T2]): LogicalBoolean =
    new BinaryOperatorNodeLogicalBoolean(this, q.copy(false, Nil).ast, "<=")

  def >[A2, T2](q: Query[A2])(implicit cc: CanCompare[T1, T2]): LogicalBoolean = gt(q)
  def >=[A2, T2](q: Query[A2])(implicit cc: CanCompare[T1, T2]): LogicalBoolean = gte(q)
  def <[A2, T2](q: Query[A2])(implicit cc: CanCompare[T1, T2]): LogicalBoolean = lt(q)
  def <=[A2, T2](q: Query[A2])(implicit cc: CanCompare[T1, T2]): LogicalBoolean = lte(q)

  def >[A2, T2](b: TypedExpression[A2, T2])(implicit ev: CanCompare[T1, T2]) = gt(b)
  def <[A2, T2](b: TypedExpression[A2, T2])(implicit ev: CanCompare[T1, T2]) = lt(b)
  def >=[A2, T2](b: TypedExpression[A2, T2])(implicit ev: CanCompare[T1, T2]) = gte(b)
  def <=[A2, T2](b: TypedExpression[A2, T2])(implicit ev: CanCompare[T1, T2]) = lte(b)

  // TODO: add T1 <:< TOption to isNull and isNotNull
  def isNull = new PostfixOperatorNode("is null", this) with LogicalBoolean
  def isNotNull = new PostfixOperatorNode("is not null", this) with LogicalBoolean

  def between[A2, T2, A3, T3](b1: TypedExpression[A2, T2], b2: TypedExpression[A3, T3])(implicit
    ev1: CanCompare[T1, T2],
    ev2: CanCompare[T2, T3]
  ) = new BetweenExpression(this, b1, b2)

  def like[A2, T2 <: TOptionString](s: TypedExpression[A2, T2])(implicit ev: CanCompare[T1, T2]) =
    new BinaryOperatorNodeLogicalBoolean(this, s, "like")

  def ilike[A2, T2 <: TOptionString](s: TypedExpression[A2, T2])(implicit ev: CanCompare[T1, T2]) =
    new BinaryOperatorNodeLogicalBoolean(this, s, "ilike")

  def ||[A2, T2](e: TypedExpression[A2, T2]) = new ConcatOp[A1, A2, T1, T2](this, e)

  def regex(pattern: String) = new FunctionNode(pattern, Seq(this)) with LogicalBoolean {

    override def doWrite(sw: StatementWriter) =
      Session.currentSession.databaseAdapter.writeRegexExpression(outer, pattern, sw)
  }

  def is(columnAttributes: AttributeValidOnNumericalColumn*)(implicit restrictUsageWithinSchema: Schema) =
    new ColumnAttributeAssignment(_fieldMetaData, columnAttributes)

  def in[A2, T2](t: Iterable[A2])(implicit cc: CanCompare[T1, T2]): LogicalBoolean =
    new InclusionOperator(this, new RightHandSideOfIn(new ConstantExpressionNodeList(t, mapper)).toIn)

  def in[A2, T2](q: Query[A2])(implicit cc: CanCompare[T1, T2]): LogicalBoolean =
    new InclusionOperator(this, new RightHandSideOfIn(q.copy(false, Nil).ast))

  def notIn[A2, T2](t: Iterable[A2])(implicit cc: CanCompare[T1, T2]): LogicalBoolean =
    new ExclusionOperator(this, new RightHandSideOfIn(new ConstantExpressionNodeList(t, mapper)).toNotIn)

  def notIn[A2, T2](q: Query[A2])(implicit cc: CanCompare[T1, T2]): LogicalBoolean =
    new ExclusionOperator(this, new RightHandSideOfIn(q.copy(false, Nil).ast))

  def ~ = this

  def sample: A1 = mapper.sample

  def mapper: OutMapper[A1]

  def :=[B](b: B)(implicit ev: B => TypedExpression[A1, T1]): UpdateAssignment =
    new UpdateAssignment(_fieldMetaData, ev(b): TypedExpression[A1, T1])

  def :=(q: Query[Measures[A1]]) =
    new UpdateAssignment(_fieldMetaData, q.ast)

  def defaultsTo[B](b: B)(implicit ev: B => TypedExpression[A1, T1]): DefaultValueAssignment =
    new DefaultValueAssignment(_fieldMetaData, ev(b): TypedExpression[A1, T1])

  /**
   * TODO: make safer with compiler plugin
   * Not type safe ! a TypedExpressionNode[T] might not be a SelectElementReference[_] that refers to a FieldSelectElement...   
   */
  private[squeryl] def _fieldMetaData = {
    val ser =
      try {
        this.asInstanceOf[SelectElementReference[_, _]]
      } catch { // TODO: validate this at compile time with a scalac plugin
        case e: ClassCastException => {
          throw new RuntimeException(
            "left side of assignment '" + Utils.failSafeString(
              this.toString
            ) + "' is invalid, make sure statement uses *only* closure argument.",
            e
          )
        }
      }

    val fmd =
      try {
        ser.selectElement.asInstanceOf[FieldSelectElement].fieldMetaData
      } catch { // TODO: validate this at compile time with a scalac plugin
        case e: ClassCastException => {
          throw new RuntimeException(
            "left side of assignment '" + Utils.failSafeString(
              this.toString
            ) + "' is invalid, make sure statement uses *only* closure argument.",
            e
          )
        }
      }
    fmd
  }
}

/**
 * Wraps an existing [[ast.ExpressionNode]] and reinterprets it as a [[TypedExpression]]
 * with a specific Scala type and type tag. This is used internally by
 * [[TypedExpressionFactory.convert]] to apply type-level conversions without changing
 * the underlying SQL generation.
 *
 * @param e  the underlying expression node to wrap
 * @param bf the factory that provides the [[org.squeryl.internals.OutMapper]] for type `A1`
 * @tparam A1 the Scala type this conversion yields
 * @tparam T1 the type tag for this expression
 */
class TypedExpressionConversion[A1, T1](val e: ExpressionNode, bf: TypedExpressionFactory[A1, T1])
    extends TypedExpression[A1, T1] {

  def mapper: OutMapper[A1] = bf.createOutMapper

  override def inhibited = e.inhibited

  override def doWrite(sw: StatementWriter) = e.doWrite((sw))

  override def children = e.children
}

/**
 * Type class that promotes an expression to a floating-point type when division is performed.
 * For integral types (e.g., `TInt`, `TLong`), the result of division is promoted to a
 * floating-point type (e.g., `TFloat`, `TDouble`). For types that are already floating-point,
 * the identity floatifier is used and no promotion occurs.
 *
 * @tparam T1 the type tag of the input expression (the dividend's resolved type)
 * @tparam A2 the Scala type of the promoted result
 * @tparam T2 the type tag of the promoted result
 */
trait Floatifier[T1, A2, T2] {

  /** Wraps the given expression node in a typed conversion to the floating-point result type. */
  def floatify(v: ExpressionNode): TypedExpressionConversion[A2, T2]
}

/**
 * A [[Floatifier]] that maps a type to itself, used for types that are already
 * floating-point (e.g., `Float`, `Double`, `BigDecimal`).
 */
trait IdentityFloatifier[A1, T1] extends Floatifier[T1, A1, T1]

/**
 * A [[TypedExpressionFactory]] for floating-point types that acts as its own identity
 * floatifier. Division of a floating-point expression yields the same floating-point type.
 */
trait FloatTypedExpressionFactory[A1, T1] extends TypedExpressionFactory[A1, T1] with IdentityFloatifier[A1, T1] {
  self: JdbcMapper[_, A1] =>
  def floatify(v: ExpressionNode): TypedExpressionConversion[A1, T1] = convert(v)
}

/**
 * Maps between a JDBC-level primitive type `P` and a Scala application type `A`.
 * Provides the bridge between `ResultSet` extraction and Scala value construction,
 * as well as the reverse conversion for parameter binding.
 *
 * @tparam P the JDBC primitive type (e.g., `java.sql.Date`, `Int`, `String`)
 * @tparam A the Scala application type (e.g., `java.time.LocalDate`, `Int`, custom types)
 */
trait JdbcMapper[P, A] {
  self: TypedExpressionFactory[A, _] =>
  def thisTypedExpressionFactory: TypedExpressionFactory[A, _] = this

  /** Extracts the raw JDBC value from the result set at the given column index. */
  def extractNativeJdbcValue(rs: ResultSet, i: Int): P

  /** Converts a JDBC primitive value to the Scala application type. */
  def convertFromJdbc(v: P): A

  /** Converts a Scala application value to the JDBC primitive type for parameter binding. */
  def convertToJdbc(v: A): P

  /** The default column length used in DDL generation for this type. */
  def defaultColumnLength: Int

  /** Reads a value from the result set by extracting and converting the JDBC primitive. */
  def map(rs: ResultSet, i: Int): A = convertFromJdbc(extractNativeJdbcValue(rs, i))
}

/**
 * A [[JdbcMapper]] specialization for SQL array types. Determines the native JDBC type
 * by inspecting the sample value at runtime.
 *
 * @tparam P the JDBC array type (typically `java.sql.Array`)
 * @tparam A the Scala array type (e.g., `Array[Int]`, `Array[String]`)
 */
trait ArrayJdbcMapper[P, A] extends JdbcMapper[P, A] {
  self: TypedExpressionFactory[A, _] =>
  def nativeJdbcType = sample.asInstanceOf[AnyRef].getClass
}

/**
 * A [[JdbcMapper]] where the JDBC type and the Scala type are identical, so
 * `convertFromJdbc` and `convertToJdbc` are identity functions. Used for types
 * like `Int`, `Long`, `String`, `Double`, etc. that map directly to JDBC types.
 *
 * @tparam A the type that is both the JDBC primitive and the Scala application type
 */
trait PrimitiveJdbcMapper[A] extends JdbcMapper[A, A] {
  self: TypedExpressionFactory[A, _] =>
  def extractNativeJdbcValue(rs: ResultSet, i: Int): A
  def convertFromJdbc(v: A) = v
  def convertToJdbc(v: A) = v
  def nativeJdbcType = sample.asInstanceOf[AnyRef].getClass
}

/**
 * A [[JdbcMapper]] for non-primitive types that are stored as a JDBC primitive `P` but
 * represented in Scala as a different type `A`. Delegates JDBC extraction and column
 * length to an underlying [[PrimitiveJdbcMapper]], and registers itself with the
 * [[org.squeryl.internals.FieldMapper]] so that the custom type is recognized during schema introspection.
 *
 * Subclasses must implement `convertFromJdbc` and `convertToJdbc` to define the
 * bidirectional mapping between `P` and `A`.
 *
 * @param primitiveMapper the underlying mapper for the JDBC primitive type
 * @param fieldMapper     the field mapper to register this custom type with
 * @tparam P the JDBC primitive type
 * @tparam A the Scala application type
 * @tparam T the type tag for the expression system
 */
abstract class NonPrimitiveJdbcMapper[P, A, T](
  val primitiveMapper: PrimitiveJdbcMapper[P],
  val fieldMapper: FieldMapper
) extends JdbcMapper[P, A]
    with TypedExpressionFactory[A, T] {
  self: TypedExpressionFactory[A, T] =>

  def extractNativeJdbcValue(rs: ResultSet, i: Int): P = primitiveMapper.extractNativeJdbcValue(rs, i)
  def defaultColumnLength: Int = primitiveMapper.defaultColumnLength
  def sample: A =
    convertFromJdbc(primitiveMapper.thisTypedExpressionFactory.sample)

  /** Creates a typed expression from a native JDBC value by converting it through the mapper. */
  def createFromNativeJdbcValue(v: P) = create(convertFromJdbc(v))

  fieldMapper.register(this)
}

/**
 * Factory for creating [[TypedExpression]] instances of a specific Scala type `A` and
 * type tag `T`. Serves as the central integration point between the Scala type system,
 * the JDBC mapping layer, and the expression AST.
 *
 * When `create` is called, it checks whether the value originated from a field access
 * (via [[org.squeryl.internals.FieldReferenceLinker]]). If so, it produces a [[org.squeryl.dsl.ast.SelectElementReference]] that
 * refers to the column; otherwise, it produces a [[org.squeryl.dsl.ast.ConstantTypedExpression]].
 *
 * Also provides `convert` to re-type an existing AST node, `createOutMapper` to build
 * result set mappers, and `jdbcSample` for DDL introspection.
 *
 * @tparam A the Scala type produced by this factory
 * @tparam T the type tag for expressions created by this factory
 */
trait TypedExpressionFactory[A, T] {
  self: JdbcMapper[_, A] =>

  def thisAnyRefMapper = this.asInstanceOf[JdbcMapper[AnyRef, A]]

  /**
   * Creates a [[TypedExpression]] for the given value. If a field reference was just
   * accessed (indicating this value comes from a table column), produces a
   * [[org.squeryl.dsl.ast.SelectElementReference]]; otherwise produces a constant expression.
   */
  def create(a: A): TypedExpression[A, T] =
    FieldReferenceLinker.takeLastAccessedFieldReference match {
      case None =>
        createConstant(a)
      case Some(n: SelectElement) =>
        new SelectElementReference[A, T](n, createOutMapper)
    }

  /** Creates a constant SQL expression for the given Scala value. */
  def createConstant(a: A) =
    new ConstantTypedExpression[A, T](a, thisAnyRefMapper.convertToJdbc(a), Some(this))

  /** Returns the JDBC representation of this factory's sample value. */
  def jdbcSample =
    thisAnyRefMapper.convertToJdbc(sample)

  /**
   * Converts the argument into a TypedExpression[A,T], the resulting expression
   * is meant to be equivalent in terms of SQL generation, the conversion is only
   * at the type level
   */
  def convert(v: ExpressionNode) = new TypedExpressionConversion[A, T](v, this)

  def sample: A

  def defaultColumnLength: Int

  def thisMapper: JdbcMapper[_, A] = this

  private def zis = this

  def createOutMapper: OutMapper[A] = new OutMapper[A] {

    def doMap(rs: ResultSet): A =
      zis.map(rs, index)

    def sample: A = zis.sample
  }
}

/**
 * A [[TypedExpressionFactory]] for integral types (byte, int, long) that also implements
 * [[Floatifier]] to promote division results to a floating-point type. For example,
 * dividing two `Int` expressions yields a `Float` result.
 *
 * @tparam A1 the Scala integral type
 * @tparam T1 the integral type tag
 * @tparam A2 the Scala floating-point type that division promotes to
 * @tparam T2 the floating-point type tag that division promotes to
 */
trait IntegralTypedExpressionFactory[A1, T1, A2, T2]
    extends TypedExpressionFactory[A1, T1]
    with Floatifier[T1, A2, T2] {
  self: JdbcMapper[_, A1] =>

  /** Promotes the expression to the floating-point result type for division. */
  def floatify(v: ExpressionNode): TypedExpressionConversion[A2, T2] = floatifyer.convert(v)

  /** The factory for the floating-point type that division results are promoted to. */
  def floatifyer: TypedExpressionFactory[A2, T2]
}

/**
 * Bridges a non-optional typed expression factory to its `Option`-wrapped counterpart.
 * Given a factory for type `A1` with tag `T1`, this trait creates a factory for
 * `Option[A1]` with tag `T2` by delegating all JDBC operations to the underlying
 * `deOptionizer` and wrapping/unwrapping with `Option`.
 *
 * The `createOutMapper` override handles SQL NULL detection via `ResultSet.wasNull`,
 * returning `None` for null database values.
 *
 * @tparam P1 the JDBC primitive type
 * @tparam A1 the non-optional Scala type
 * @tparam T1 the non-optional type tag
 * @tparam A2 `Option[A1]` (constrained by type bounds)
 * @tparam T2 the optional type tag (e.g., `TOptionInt` for `TInt`)
 */
trait DeOptionizer[P1, A1, T1, A2 >: Option[A1] <: Option[A1], T2] extends JdbcMapper[P1, A2] {
  self: TypedExpressionFactory[A2, T2] =>

  def deOptionizer: TypedExpressionFactory[A1, T1] with JdbcMapper[P1, A1]

  def sample = Option(deOptionizer.sample)

  def defaultColumnLength: Int = deOptionizer.defaultColumnLength

  def convertFromJdbc(v: P1): A2 = Option(deOptionizer.convertFromJdbc(v))

  def convertToJdbc(v: A2): P1 =
    v map (p => deOptionizer.convertToJdbc(p)) getOrElse (null.asInstanceOf[P1])

  def extractNativeJdbcValue(rs: ResultSet, i: Int) = deOptionizer.extractNativeJdbcValue(rs, i)

  override def createOutMapper: OutMapper[A2] = new OutMapper[A2] {
    def doMap(rs: ResultSet): A2 = {

      val v = deOptionizer.thisMapper.map(rs, index)
      val r =
        if (rs.wasNull)
          None
        else
          Option(v)

      r
    }

    def sample: A2 = Option(deOptionizer.sample)
  }
}

/**
 * AST node for the SQL concatenation operator (`||`). Delegates rendering to the
 * database adapter's `writeConcatOperator` method, which may vary across databases
 * (e.g., `||` in PostgreSQL, `CONCAT()` in MySQL).
 */
class ConcatOp[A1, A2, T1, T2](val a1: TypedExpression[A1, T1], val a2: TypedExpression[A2, T2])
    extends BinaryOperatorNode(a1, a2, "||") {
  override def doWrite(sw: StatementWriter) =
    sw.databaseAdapter.writeConcatOperator(a1, a2, sw)
}

/**
 * AST node for the SQL `NVL`/`COALESCE` function. Returns the first non-null value
 * among its arguments. Delegates rendering to the database adapter's `writeNvlCall`
 * method. The result type is that of the second (non-optional) expression `e2`.
 *
 * @tparam A the Scala result type
 * @tparam T the type tag of the result
 */
class NvlNode[A, T](e1: TypedExpression[_, _], e2: TypedExpression[A, T])
    extends BinaryOperatorNode(e1, e2, "nvl", false)
    with TypedExpression[A, T] {

  def mapper = e2.mapper

  override def doWrite(sw: StatementWriter) =
    sw.databaseAdapter.writeNvlCall(left, right, sw)
}
