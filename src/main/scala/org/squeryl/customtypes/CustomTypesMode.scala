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
package org.squeryl.customtypes;

import java.util.{Date, UUID}
import org.squeryl.dsl._
import java.sql.Timestamp
import org.squeryl.internals.FieldMapper

/**
 * Base trait for user-defined custom field types that wrap a primitive value. Custom types
 * allow defining domain-specific types (e.g., `EmailAddress`, `Currency`) that are stored
 * as primitive JDBC types but provide type safety in the Scala layer.
 *
 * Extends `Product1[T]` so custom types can participate in pattern matching and
 * case-class-like destructuring.
 *
 * Usage: create a class extending `CustomType[T]` (e.g., `class EmailField(val value: String) extends CustomType[String]`),
 * then register the corresponding TEF (typed expression factory) in a [[CustomTypesMode]] trait.
 *
 * @tparam T the underlying primitive type (e.g., `String`, `Int`, `Long`)
 */
trait CustomType[T] extends Product1[T] {
  /** The underlying primitive value. */
  def value: T
  def _1 = value
  def canEqual(a: Any) = false
}

/**
 * A Squeryl DSL mode that enables the use of [[CustomType]] wrapper classes as column types.
 * Provides pre-built typed expression factories (TEFs) and implicit conversions for all
 * standard custom field types: `StringField`, `DateField`, `TimestampField`, `BooleanField`,
 * `UuidField`, `ByteField`, `IntField`, `LongField`, `FloatField`, `DoubleField`, and
 * `BigDecimalField`.
 *
 * To use custom types, extend this trait (or use the `CustomTypesMode` companion object)
 * instead of the standard `PrimitiveTypeMode`. Each custom field class wraps a primitive
 * value and is mapped to/from JDBC via [[org.squeryl.dsl.NonPrimitiveJdbcMapper]] instances defined here.
 *
 * Implicit conversions allow seamless interoperability between raw primitive values and
 * their custom type wrappers in query expressions.
 */
trait CustomTypesMode extends QueryDsl with FieldMapper {

  private[this] val ps = PrimitiveTypeSupport

  val stringTEF = new NonPrimitiveJdbcMapper[String, StringField, TString](ps.stringTEF, this) {
    def convertFromJdbc(v: String) = new StringField(v)
    def convertToJdbc(v: StringField) = v.value
  }

  val optionStringTEF = new TypedExpressionFactory[Option[StringField], TOptionString]
    with DeOptionizer[String, StringField, TString, Option[StringField], TOptionString] {
    val deOptionizer: TypedExpressionFactory[StringField, TString] with JdbcMapper[String, StringField] = stringTEF
  }

  val dateTEF = new NonPrimitiveJdbcMapper[Date, DateField, TDate](ps.dateTEF, this) {
    def convertFromJdbc(v: Date) = new DateField(v)
    def convertToJdbc(v: DateField) = v.value
  }

  val optionDateTEF = new TypedExpressionFactory[Option[DateField], TOptionDate]
    with DeOptionizer[Date, DateField, TDate, Option[DateField], TOptionDate] {
    val deOptionizer: TypedExpressionFactory[DateField, TDate] with JdbcMapper[Date, DateField] = dateTEF
  }

  val timestampTEF = new NonPrimitiveJdbcMapper[Timestamp, TimestampField, TTimestamp](ps.timestampTEF, this) {
    def convertFromJdbc(v: Timestamp) = new TimestampField(v)
    def convertToJdbc(v: TimestampField) = v.value

  }

  val optionTimestampTEF = new TypedExpressionFactory[Option[TimestampField], TOptionTimestamp]
    with DeOptionizer[Timestamp, TimestampField, TTimestamp, Option[TimestampField], TOptionTimestamp] {
    val deOptionizer: TypedExpressionFactory[TimestampField, TTimestamp] with JdbcMapper[Timestamp, TimestampField] =
      timestampTEF
  }

  val booleanTEF = new NonPrimitiveJdbcMapper[Boolean, BooleanField, TBoolean](ps.booleanTEF, this) {
    def convertFromJdbc(v: Boolean) = new BooleanField(v)
    def convertToJdbc(v: BooleanField) = v.value
  }

  val optionBooleanTEF = new TypedExpressionFactory[Option[BooleanField], TOptionBoolean]
    with DeOptionizer[Boolean, BooleanField, TBoolean, Option[BooleanField], TOptionBoolean] {
    val deOptionizer: TypedExpressionFactory[BooleanField, TBoolean] with JdbcMapper[Boolean, BooleanField] = booleanTEF
  }

  val uuidTEF = new NonPrimitiveJdbcMapper[UUID, UuidField, TUUID](ps.uuidTEF, this) {
    def convertFromJdbc(v: UUID) = new UuidField(v)
    def convertToJdbc(v: UuidField) = v.value
  }

  val optionUUIDTEF = new TypedExpressionFactory[Option[UuidField], TOptionUUID]
    with DeOptionizer[UUID, UuidField, TUUID, Option[UuidField], TOptionUUID] {
    val deOptionizer: TypedExpressionFactory[UuidField, TUUID] with JdbcMapper[UUID, UuidField] = uuidTEF
  }

  // =========================== Numerical Integral ===========================

  val byteTEF = new NonPrimitiveJdbcMapper[Byte, ByteField, TByte](ps.byteTEF, this) {
    def convertFromJdbc(v: Byte) = new ByteField(v)
    def convertToJdbc(v: ByteField) = v.value
  }

  val optionByteTEF =
    new IntegralTypedExpressionFactory[Option[ByteField], TOptionByte, Option[FloatField], TOptionFloat]
      with DeOptionizer[Byte, ByteField, TByte, Option[ByteField], TOptionByte] {
      val deOptionizer: TypedExpressionFactory[ByteField, TByte] with JdbcMapper[Byte, ByteField] = byteTEF
      val floatifyer: TypedExpressionFactory[Option[FloatField], TOptionFloat] = optionFloatTEF
    }

  val intTEF = new NonPrimitiveJdbcMapper[Int, IntField, TInt](ps.intTEF, this)
    with IntegralTypedExpressionFactory[IntField, TInt, FloatField, TFloat] {
    val floatifyer: TypedExpressionFactory[FloatField, TFloat] = floatTEF
    def convertFromJdbc(v: Int) = new IntField(v)
    def convertToJdbc(v: IntField) = v.value
  }

  val optionIntTEF = new IntegralTypedExpressionFactory[Option[IntField], TOptionInt, Option[FloatField], TOptionFloat]
    with DeOptionizer[Int, IntField, TInt, Option[IntField], TOptionInt] {
    val deOptionizer: TypedExpressionFactory[IntField, TInt] with JdbcMapper[Int, IntField] = intTEF
    val floatifyer: TypedExpressionFactory[Option[FloatField], TOptionFloat] = optionFloatTEF
  }

  val longTEF = new NonPrimitiveJdbcMapper[Long, LongField, TLong](ps.longTEF, this)
    with IntegralTypedExpressionFactory[LongField, TLong, DoubleField, TDouble] {
    val floatifyer: TypedExpressionFactory[DoubleField, TDouble] = doubleTEF
    def convertFromJdbc(v: Long) = new LongField(v)
    def convertToJdbc(v: LongField) = v.value
  }

  val optionLongTEF =
    new IntegralTypedExpressionFactory[Option[LongField], TOptionLong, Option[DoubleField], TOptionDouble]
      with DeOptionizer[Long, LongField, TLong, Option[LongField], TOptionLong] {
      val deOptionizer: TypedExpressionFactory[LongField, TLong] with JdbcMapper[Long, LongField] = longTEF
      val floatifyer: TypedExpressionFactory[Option[DoubleField], TOptionDouble] = optionDoubleTEF
    }

  // =========================== Numerical Floating Point ===========================

  val floatTEF = new NonPrimitiveJdbcMapper[Float, FloatField, TFloat](ps.floatTEF, this)
    with FloatTypedExpressionFactory[FloatField, TFloat] {
    def convertFromJdbc(v: Float) = new FloatField(v)
    def convertToJdbc(v: FloatField) = v.value
  }

  val optionFloatTEF = new FloatTypedExpressionFactory[Option[FloatField], TOptionFloat]
    with DeOptionizer[Float, FloatField, TFloat, Option[FloatField], TOptionFloat] {
    val deOptionizer: TypedExpressionFactory[FloatField, TFloat] with JdbcMapper[Float, FloatField] = floatTEF
  }

  val doubleTEF = new NonPrimitiveJdbcMapper[Double, DoubleField, TDouble](ps.doubleTEF, this)
    with FloatTypedExpressionFactory[DoubleField, TDouble] {
    def convertFromJdbc(v: Double) = new DoubleField(v)
    def convertToJdbc(v: DoubleField) = v.value
  }

  val optionDoubleTEF = new FloatTypedExpressionFactory[Option[DoubleField], TOptionDouble]
    with DeOptionizer[Double, DoubleField, TDouble, Option[DoubleField], TOptionDouble] {
    val deOptionizer: TypedExpressionFactory[DoubleField, TDouble] with JdbcMapper[Double, DoubleField] = doubleTEF
  }

  val bigDecimalTEF = new NonPrimitiveJdbcMapper[BigDecimal, BigDecimalField, TBigDecimal](ps.bigDecimalTEF, this)
    with FloatTypedExpressionFactory[BigDecimalField, TBigDecimal] {
    def convertFromJdbc(v: BigDecimal) = new BigDecimalField(v)
    def convertToJdbc(v: BigDecimalField) = v.value
  }

  val optionBigDecimalTEF = new FloatTypedExpressionFactory[Option[BigDecimalField], TOptionBigDecimal]
    with DeOptionizer[BigDecimal, BigDecimalField, TBigDecimal, Option[BigDecimalField], TOptionBigDecimal] {
    val deOptionizer
      : TypedExpressionFactory[BigDecimalField, TBigDecimal] with JdbcMapper[BigDecimal, BigDecimalField] =
      bigDecimalTEF
  }

  implicit def stringToTE(s: String): TypedExpression[StringField, TString] = stringTEF.createFromNativeJdbcValue(s)
  implicit def optionStringToTE(s: Option[String]): Option[org.squeryl.customtypes.StringField] =
    s.map(new StringField(_))

  implicit def dateToTE(s: Date): TypedExpression[DateField, TDate] = dateTEF.createFromNativeJdbcValue(s)
  implicit def optionDateToTE(s: Option[Date]): Option[org.squeryl.customtypes.DateField] = s.map(new DateField(_))

  implicit def timestampToTE(s: Timestamp): TypedExpression[TimestampField, TTimestamp] =
    timestampTEF.createFromNativeJdbcValue(s)
  implicit def optionTimestampToTE(s: Option[Timestamp]): Option[org.squeryl.customtypes.TimestampField] =
    s.map(new TimestampField(_))

  implicit def booleanToTE(s: Boolean): TypedExpression[BooleanField, TBoolean] =
    booleanTEF.createFromNativeJdbcValue(s)
  implicit def optionBooleanToTE(s: Option[Boolean]): Option[org.squeryl.customtypes.BooleanField] =
    s.map(new BooleanField(_))

  implicit def uuidToTE(s: UUID): TypedExpression[UuidField, TUUID] = uuidTEF.createFromNativeJdbcValue(s)
  implicit def optionUUIDToTE(s: Option[UUID]): Option[org.squeryl.customtypes.UuidField] = s.map(new UuidField(_))

  implicit def byteToTE(f: Byte): TypedExpression[ByteField, TByte] = byteTEF.createFromNativeJdbcValue(f)
  implicit def optionByteToTE(f: Option[Byte]): Option[org.squeryl.customtypes.ByteField] = f.map(new ByteField(_))

  implicit def intToTE(f: IntField): TypedExpression[IntField, TInt] = intTEF.create(f)
  implicit def optionIntToTE(f: Option[IntField]): TypedExpression[Option[IntField], TOptionInt] =
    optionIntTEF.create(f)

  // implicit def _intToTE(f: Int) = intTEF.createFromNativeJdbcValue(f)
  // implicit def _optionIntToTE(f: Option[Int]) = f.map(new IntField(_))

  implicit def longToTE(f: Long): TypedExpression[LongField, TLong] = longTEF.createFromNativeJdbcValue(f)
  implicit def optionLongToTE(f: Option[Long]): Option[org.squeryl.customtypes.LongField] = f.map(new LongField(_))

  implicit def floatToTE(f: Float): TypedExpression[FloatField, TFloat] = floatTEF.createFromNativeJdbcValue(f)
  implicit def optionFloatToTE(f: Option[Float]): Option[org.squeryl.customtypes.FloatField] = f.map(new FloatField(_))

  implicit def doubleToTE(f: Double): TypedExpression[DoubleField, TDouble] = doubleTEF.createFromNativeJdbcValue(f)
  implicit def optionDoubleToTE(f: Option[Double]): Option[org.squeryl.customtypes.DoubleField] =
    f.map(new DoubleField(_))

  implicit def bigDecimalToTE(f: BigDecimal): TypedExpression[BigDecimalField, TBigDecimal] =
    bigDecimalTEF.createFromNativeJdbcValue(f)
  implicit def optionBigDecimalToTE(f: Option[BigDecimal]): Option[org.squeryl.customtypes.BigDecimalField] =
    f.map(new BigDecimalField(_))
}

/** Default instance of [[CustomTypesMode]] for use as an import-based DSL entry point. */
object CustomTypesMode extends CustomTypesMode

/** Custom type wrapping a `Byte` value for type-safe byte columns. */
class ByteField(val value: Byte) extends CustomType[Byte]

/** Custom type wrapping an `Int` value for type-safe integer columns. */
class IntField(val value: Int) extends CustomType[Int]

/** Custom type wrapping a `String` value for type-safe string columns. */
class StringField(val value: String) extends CustomType[String]

/** Custom type wrapping a `Double` value for type-safe double-precision columns. */
class DoubleField(val value: Double) extends CustomType[Double]

/** Custom type wrapping a `BigDecimal` value for type-safe decimal columns. */
class BigDecimalField(val value: BigDecimal) extends CustomType[BigDecimal]

/** Custom type wrapping a `Float` value for type-safe single-precision columns. */
class FloatField(val value: Float) extends CustomType[Float]

/** Custom type wrapping a `Long` value for type-safe long integer columns. */
class LongField(val value: Long) extends CustomType[Long]

/** Custom type wrapping a `Boolean` value for type-safe boolean columns. */
class BooleanField(val value: Boolean) extends CustomType[Boolean]

/** Custom type wrapping a `java.util.Date` value for type-safe date columns. */
class DateField(val value: Date) extends CustomType[Date]

/** Custom type wrapping a `java.sql.Timestamp` value for type-safe timestamp columns. */
class TimestampField(val value: Timestamp) extends CustomType[Timestamp]

/** Custom type wrapping an `Array[Byte]` value for type-safe binary/blob columns. */
class BinaryField(val value: Array[Byte]) extends CustomType[Array[Byte]]

/** Custom type wrapping a `java.util.UUID` value for type-safe UUID columns. */
class UuidField(val value: UUID) extends CustomType[UUID]
