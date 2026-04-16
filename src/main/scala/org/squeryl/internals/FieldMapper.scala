/** *****************************************************************************
 * Copyright 2010 Maxime Lévesque
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * **************************************************************************** */

package org.squeryl.internals

import java.sql.{ResultSet, Timestamp}
import java.time._
import java.util.{Date, UUID}
import org.squeryl.TargetsValuesSupertype
import org.squeryl.dsl.{ArrayJdbcMapper, _}

import java.sql
import scala.annotation.tailrec
import scala.collection.mutable.HashMap

/**
 * Central registry that maps Scala/Java types to their JDBC representations and back.
 *
 * Every type that Squeryl can persist must be registered here via a [[org.squeryl.dsl.TypedExpressionFactory]]
 * paired with a [[org.squeryl.dsl.JdbcMapper]]. The registry is populated during construction by
 * [[initialize]], which registers all built-in primitive types (numerics, strings, dates, UUIDs, etc.).
 * Custom (non-primitive) types are registered separately through the public and package-private
 * `register` overloads.
 *
 * Concrete type modes (e.g. [[org.squeryl.PrimitiveTypeMode]]) extend this trait and expose the
 * [[PrimitiveTypeSupport]] members as implicits, making those types available in queries and schema
 * definitions.
 */
trait FieldMapper {
  outer =>

  private[this] val registry = new HashMap[Class[_], FieldAttributesBasedOnType[_]]

  implicit def thisFieldMapper: FieldMapper = this

  /**
   * Extending classes will expose members of PrimitiveTypeSupport as implicit, to enable
   * support of primitive types, or will expose theit own non jdbc native types.
   */

  /**
   * Contains all built-in typed expression factories (TEFs) for primitive JDBC types and their
   * `Option` counterparts. Each TEF defines how a Scala type is read from a `ResultSet`,
   * what its default column length is, and how it converts to/from JDBC values.
   *
   * Types defined here are not implicitly available on their own; a concrete type mode
   * (e.g. [[org.squeryl.PrimitiveTypeMode]]) must re-export them as implicits.
   */
  protected object PrimitiveTypeSupport {
    // =========================== Non Numerical ===========================

    val stringTEF: TypedExpressionFactory[String, TString] with PrimitiveJdbcMapper[String] =
      new TypedExpressionFactory[String, TString] with PrimitiveJdbcMapper[String] {
        val sample: String = "": String
        val defaultColumnLength = 128

        def extractNativeJdbcValue(rs: ResultSet, i: Int): String = rs.getString(i)
      }

    val optionStringTEF: TypedExpressionFactory[Option[String], TOptionString]
      with DeOptionizer[String, String, TString, Option[String], TOptionString] =
      new TypedExpressionFactory[Option[String], TOptionString]
        with DeOptionizer[String, String, TString, Option[String], TOptionString] {
        val deOptionizer: TypedExpressionFactory[String, TString] with JdbcMapper[String, String] = stringTEF
      }

    val dateTEF: TypedExpressionFactory[Date, TDate] with PrimitiveJdbcMapper[Date] =
      new TypedExpressionFactory[Date, TDate] with PrimitiveJdbcMapper[Date] {
        val sample = new Date
        val defaultColumnLength: Int = -1

        def extractNativeJdbcValue(rs: ResultSet, i: Int): Date = rs.getDate(i)
      }

    val sqlDateTEF: TypedExpressionFactory[sql.Date, TDate] with PrimitiveJdbcMapper[sql.Date] =
      new TypedExpressionFactory[java.sql.Date, TDate] with PrimitiveJdbcMapper[java.sql.Date] {
        val sample = new java.sql.Date(0L)
        val defaultColumnLength: Int = -1

        def extractNativeJdbcValue(rs: ResultSet, i: Int): sql.Date = rs.getDate(i)
      }

    val localDateTEF: TypedExpressionFactory[LocalDate, TLocalDate] with PrimitiveJdbcMapper[LocalDate] =
      new TypedExpressionFactory[LocalDate, TLocalDate] with PrimitiveJdbcMapper[LocalDate] {
        val sample: LocalDate = LocalDate.now()
        val defaultColumnLength: Int = -1

        def extractNativeJdbcValue(rs: ResultSet, i: Int): LocalDate = rs.getObject(i, classOf[LocalDate])
      }

    val localTimeTEF: TypedExpressionFactory[LocalTime, TLocalTime] with PrimitiveJdbcMapper[LocalTime] =
      new TypedExpressionFactory[LocalTime, TLocalTime] with PrimitiveJdbcMapper[LocalTime] {
        val sample: LocalTime = LocalTime.now()
        val defaultColumnLength: Int = -1

        def extractNativeJdbcValue(rs: ResultSet, i: Int): LocalTime = rs.getObject(i, classOf[LocalTime])
      }

    val optionDateTEF: TypedExpressionFactory[Option[Date], TOptionDate]
      with DeOptionizer[Date, Date, TDate, Option[Date], TOptionDate] =
      new TypedExpressionFactory[Option[Date], TOptionDate]
        with DeOptionizer[Date, Date, TDate, Option[Date], TOptionDate] {
        val deOptionizer: TypedExpressionFactory[Date, TDate] with JdbcMapper[Date, Date] = dateTEF
      }

    val optionSqlDateTEF: TypedExpressionFactory[Option[sql.Date], TOptionDate]
      with DeOptionizer[sql.Date, sql.Date, TDate, Option[sql.Date], TOptionDate] =
      new TypedExpressionFactory[Option[java.sql.Date], TOptionDate]
        with DeOptionizer[java.sql.Date, java.sql.Date, TDate, Option[java.sql.Date], TOptionDate] {
        val deOptionizer: TypedExpressionFactory[sql.Date, TDate] with JdbcMapper[sql.Date, sql.Date] = sqlDateTEF
      }

    val optionLocalDateTEF: TypedExpressionFactory[Option[LocalDate], TOptionLocalDate]
      with DeOptionizer[LocalDate, LocalDate, TLocalDate, Option[LocalDate], TOptionLocalDate] =
      new TypedExpressionFactory[Option[LocalDate], TOptionLocalDate]
        with DeOptionizer[LocalDate, LocalDate, TLocalDate, Option[LocalDate], TOptionLocalDate] {
        override val deOptionizer: TypedExpressionFactory[LocalDate, TLocalDate] with JdbcMapper[LocalDate, LocalDate] =
          localDateTEF
      }

    val optionLocalTimeTEF: TypedExpressionFactory[Option[LocalTime], TOptionLocalTime]
      with DeOptionizer[LocalTime, LocalTime, TLocalTime, Option[LocalTime], TOptionLocalTime] =
      new TypedExpressionFactory[Option[LocalTime], TOptionLocalTime]
        with DeOptionizer[LocalTime, LocalTime, TLocalTime, Option[LocalTime], TOptionLocalTime] {
        override val deOptionizer: TypedExpressionFactory[LocalTime, TLocalTime] with JdbcMapper[LocalTime, LocalTime] =
          localTimeTEF
      }

    val timestampTEF: TypedExpressionFactory[Timestamp, TTimestamp] with PrimitiveJdbcMapper[Timestamp] =
      new TypedExpressionFactory[Timestamp, TTimestamp] with PrimitiveJdbcMapper[Timestamp] {
        val sample = new Timestamp(0)
        val defaultColumnLength: Int = -1

        def extractNativeJdbcValue(rs: ResultSet, i: Int): Timestamp = rs.getTimestamp(i)
      }

    val localDateTimeTEF
      : TypedExpressionFactory[LocalDateTime, TLocalDateTime] with PrimitiveJdbcMapper[LocalDateTime] =
      new TypedExpressionFactory[LocalDateTime, TLocalDateTime] with PrimitiveJdbcMapper[LocalDateTime] {
        val sample: LocalDateTime = LocalDateTime.now()
        val defaultColumnLength: Int = -1

        def extractNativeJdbcValue(rs: ResultSet, i: Int): LocalDateTime = rs.getObject(i, classOf[LocalDateTime])
      }

    val offsetTimeTEF: TypedExpressionFactory[OffsetTime, TOffsetTime] with PrimitiveJdbcMapper[OffsetTime] =
      new TypedExpressionFactory[OffsetTime, TOffsetTime] with PrimitiveJdbcMapper[OffsetTime] {
        val sample: OffsetTime = OffsetTime.now()
        val defaultColumnLength: Int = -1

        def extractNativeJdbcValue(rs: ResultSet, i: Int): OffsetTime = rs.getObject(i, classOf[OffsetTime])
      }

    val offsetDateTimeTEF
      : TypedExpressionFactory[OffsetDateTime, TOffsetDateTime] with PrimitiveJdbcMapper[OffsetDateTime] =
      new TypedExpressionFactory[OffsetDateTime, TOffsetDateTime] with PrimitiveJdbcMapper[OffsetDateTime] {
        val sample: OffsetDateTime = OffsetDateTime.now()
        val defaultColumnLength: Int = -1

        def extractNativeJdbcValue(rs: ResultSet, i: Int): OffsetDateTime = rs.getObject(i, classOf[OffsetDateTime])
      }

    /**
     * Typed expression factory for `java.time.Instant`.
     *
     * `Instant` is a non-primitive JDBC type: it is stored in the database as an `OffsetDateTime`
     * (delegating to [[offsetDateTimeTEF]]) with the offset always normalized to UTC (`ZoneOffset.UTC`).
     * On read, the `OffsetDateTime` retrieved from the driver is converted back to an `Instant`
     * via `toInstant`.
     *
     * Because the underlying storage is `OffsetDateTime`, `Instant` has no dedicated SQL type
     * declaration method in [[DatabaseAdapter]]; it reuses the `OffsetDateTime` column type.
     */
    val instantTEF: NonPrimitiveJdbcMapper[OffsetDateTime, Instant, TInstant] =
      new NonPrimitiveJdbcMapper[OffsetDateTime, Instant, TInstant](offsetDateTimeTEF, thisFieldMapper) {
        override def convertFromJdbc(v: OffsetDateTime): Instant = if (v == null) null else v.toInstant

        override def convertToJdbc(v: Instant): OffsetDateTime = if (v == null) null else v.atOffset(ZoneOffset.UTC)
      }

    val optionTimestampTEF: TypedExpressionFactory[Option[Timestamp], TOptionTimestamp]
      with DeOptionizer[Timestamp, Timestamp, TTimestamp, Option[Timestamp], TOptionTimestamp] =
      new TypedExpressionFactory[Option[Timestamp], TOptionTimestamp]
        with DeOptionizer[Timestamp, Timestamp, TTimestamp, Option[Timestamp], TOptionTimestamp] {
        val deOptionizer: TypedExpressionFactory[Timestamp, TTimestamp] with JdbcMapper[Timestamp, Timestamp] =
          timestampTEF
      }

    val optionLocalDateTimeTEF: TypedExpressionFactory[Option[LocalDateTime], TOptionLocalDateTime]
      with DeOptionizer[LocalDateTime, LocalDateTime, TLocalDateTime, Option[LocalDateTime], TOptionLocalDateTime] =
      new TypedExpressionFactory[Option[LocalDateTime], TOptionLocalDateTime]
        with DeOptionizer[LocalDateTime, LocalDateTime, TLocalDateTime, Option[LocalDateTime], TOptionLocalDateTime] {
        val deOptionizer
          : TypedExpressionFactory[LocalDateTime, TLocalDateTime] with JdbcMapper[LocalDateTime, LocalDateTime] =
          localDateTimeTEF
      }

    val optionOffsetTimeTEF: TypedExpressionFactory[Option[OffsetTime], TOptionOffsetTime]
      with DeOptionizer[OffsetTime, OffsetTime, TOffsetTime, Option[OffsetTime], TOptionOffsetTime] =
      new TypedExpressionFactory[Option[OffsetTime], TOptionOffsetTime]
        with DeOptionizer[OffsetTime, OffsetTime, TOffsetTime, Option[OffsetTime], TOptionOffsetTime] {
        val deOptionizer: TypedExpressionFactory[OffsetTime, TOffsetTime] with JdbcMapper[OffsetTime, OffsetTime] =
          offsetTimeTEF
      }

    val optionOffsetDateTimeTEF: TypedExpressionFactory[Option[OffsetDateTime], TOptionOffsetDateTime]
      with DeOptionizer[OffsetDateTime, OffsetDateTime, TOffsetDateTime, Option[
        OffsetDateTime
      ], TOptionOffsetDateTime] = new TypedExpressionFactory[Option[OffsetDateTime], TOptionOffsetDateTime]
      with DeOptionizer[
        OffsetDateTime,
        OffsetDateTime,
        TOffsetDateTime,
        Option[OffsetDateTime],
        TOptionOffsetDateTime
      ] {
      val deOptionizer
        : TypedExpressionFactory[OffsetDateTime, TOffsetDateTime] with JdbcMapper[OffsetDateTime, OffsetDateTime] =
        offsetDateTimeTEF
    }

    val optionInstantTEF: TypedExpressionFactory[Option[Instant], TOptionInstant]
      with DeOptionizer[OffsetDateTime, Instant, TInstant, Option[Instant], TOptionInstant] =
      new TypedExpressionFactory[Option[Instant], TOptionInstant]
        with DeOptionizer[OffsetDateTime, Instant, TInstant, Option[Instant], TOptionInstant] {
        val deOptionizer: TypedExpressionFactory[Instant, TInstant] with JdbcMapper[OffsetDateTime, Instant] =
          instantTEF
      }

    val booleanTEF: TypedExpressionFactory[Boolean, TBoolean] with PrimitiveJdbcMapper[Boolean] =
      new TypedExpressionFactory[Boolean, TBoolean] with PrimitiveJdbcMapper[Boolean] {
        val sample = true
        val defaultColumnLength = 1

        def extractNativeJdbcValue(rs: ResultSet, i: Int): Boolean = rs.getBoolean(i)
      }

    val optionBooleanTEF: TypedExpressionFactory[Option[Boolean], TOptionBoolean]
      with DeOptionizer[Boolean, Boolean, TBoolean, Option[Boolean], TOptionBoolean] =
      new TypedExpressionFactory[Option[Boolean], TOptionBoolean]
        with DeOptionizer[Boolean, Boolean, TBoolean, Option[Boolean], TOptionBoolean] {
        val deOptionizer: TypedExpressionFactory[Boolean, TBoolean] with JdbcMapper[Boolean, Boolean] = booleanTEF
      }

    val uuidTEF: TypedExpressionFactory[UUID, TUUID] with PrimitiveJdbcMapper[UUID] =
      new TypedExpressionFactory[UUID, TUUID] with PrimitiveJdbcMapper[UUID] {
        val sample: UUID = java.util.UUID.fromString("00000000-0000-0000-0000-000000000000")
        val defaultColumnLength = 36

        def extractNativeJdbcValue(rs: ResultSet, i: Int): UUID = {
          val v = rs.getObject(i)
          v match {
            case u: UUID => u
            case s: String => UUID.fromString(s)
            case _ => sample
          }
        }
      }

    val optionUUIDTEF: TypedExpressionFactory[Option[UUID], TOptionUUID]
      with DeOptionizer[UUID, UUID, TUUID, Option[UUID], TOptionUUID] =
      new TypedExpressionFactory[Option[UUID], TOptionUUID]
        with DeOptionizer[UUID, UUID, TUUID, Option[UUID], TOptionUUID] {
        val deOptionizer: TypedExpressionFactory[UUID, TUUID] with JdbcMapper[UUID, UUID] = uuidTEF
      }

    val binaryTEF: TypedExpressionFactory[Array[Byte], TByteArray] with PrimitiveJdbcMapper[Array[Byte]] =
      new TypedExpressionFactory[Array[Byte], TByteArray] with PrimitiveJdbcMapper[Array[Byte]] {
        val sample: Array[Byte] = Array(0: Byte)
        val defaultColumnLength = 255

        def extractNativeJdbcValue(rs: ResultSet, i: Int): Array[Byte] = rs.getBytes(i)
      }

    val optionByteArrayTEF: TypedExpressionFactory[Option[Array[Byte]], TOptionByteArray]
      with DeOptionizer[Array[Byte], Array[Byte], TByteArray, Option[Array[Byte]], TOptionByteArray] =
      new TypedExpressionFactory[Option[Array[Byte]], TOptionByteArray]
        with DeOptionizer[Array[Byte], Array[Byte], TByteArray, Option[Array[Byte]], TOptionByteArray] {
        val deOptionizer: TypedExpressionFactory[Array[Byte], TByteArray] with JdbcMapper[Array[Byte], Array[Byte]] =
          binaryTEF
      }

    val intArrayTEF: ArrayTEF[Int, TIntArray] = new ArrayTEF[Int, TIntArray] {
      val sample: Array[Int] = Array(0)

      def toWrappedJDBCType(element: Int): java.lang.Object = java.lang.Integer.valueOf(element)

      def fromWrappedJDBCType(elements: Array[java.lang.Object]): Array[Int] =
        elements.map(i => i.asInstanceOf[java.lang.Integer].toInt)
    }

    val longArrayTEF: ArrayTEF[Long, TLongArray] = new ArrayTEF[Long, TLongArray] {
      val sample: Array[Long] = Array(0L)

      def toWrappedJDBCType(element: Long): java.lang.Object = java.lang.Long.valueOf(element)

      def fromWrappedJDBCType(elements: Array[java.lang.Object]): Array[Long] =
        elements.map(i => i.asInstanceOf[java.lang.Long].toLong)
    }

    val doubleArrayTEF: ArrayTEF[Double, TDoubleArray] = new ArrayTEF[Double, TDoubleArray] {
      val sample: Array[Double] = Array(0.0)

      def toWrappedJDBCType(element: Double): java.lang.Object = java.lang.Double.valueOf(element)

      def fromWrappedJDBCType(elements: Array[java.lang.Object]): Array[Double] =
        elements.map(i => i.asInstanceOf[java.lang.Double].toDouble)
    }

    val stringArrayTEF: ArrayTEF[String, TStringArray] = new ArrayTEF[String, TStringArray] {
      val sample: Array[String] = Array("")

      def toWrappedJDBCType(element: String): java.lang.Object = new java.lang.String(element)

      def fromWrappedJDBCType(elements: Array[java.lang.Object]): Array[String] =
        elements.map(i => i.asInstanceOf[java.lang.String].toString)
    }

    // FIXME: The type soup on this was beyond my patience for now...I think we'll need an ArrayDeOptionizer
    // val optionIntArrayTEF = new TypedExpressionFactory[Option[Array[Int]],TOptionIntArray] with DeOptionizer[Array[Int], Array[Int], TIntArray, Option[Array[Int]], TOptionIntArray] {
    // val deOptionizer = intArrayTEF
    // }

    def enumValueTEF[A >: Enumeration#Value <: Enumeration#Value](
      ev: Enumeration#Value
    ): JdbcMapper[Int, A] with TypedExpressionFactory[A, TEnumValue[A]] =
      new JdbcMapper[Int, A] with TypedExpressionFactory[A, TEnumValue[A]] {

        val enu: Enumeration = Utils.enumerationForValue(ev)

        def extractNativeJdbcValue(rs: ResultSet, i: Int): Int = rs.getInt(i)

        def defaultColumnLength: Int = intTEF.defaultColumnLength

        def sample: A = ev

        def convertToJdbc(v: A): Int = v.id

        def convertFromJdbc(v: Int): A = {
          enu.values
            .find(_.id == v)
            .getOrElse(
              DummyEnum.DummyEnumerationValue
            ) // JDBC has no concept of null value for primitive types (ex. Int)
          // at this level, we mimic this JDBC flaw (the Option / None based on jdbc.wasNull will get sorted out by optionEnumValueTEF)
        }
      }

    object DummyEnum extends Enumeration {
      type DummyEnum = Value
      val DummyEnumerationValue = Value(-1, "DummyEnumerationValue")
    }

    def optionEnumValueTEF[A >: Enumeration#Value <: Enumeration#Value](
      ev: Option[Enumeration#Value]
    ): TypedExpressionFactory[Option[A], TOptionEnumValue[A]]
      with DeOptionizer[Int, A, TEnumValue[A], Option[A], TOptionEnumValue[A]] =
      new TypedExpressionFactory[Option[A], TOptionEnumValue[A]]
        with DeOptionizer[Int, A, TEnumValue[A], Option[A], TOptionEnumValue[A]] {
        val deOptionizer: TypedExpressionFactory[A, TEnumValue[A]] with JdbcMapper[Int, A] = {
          val e = ev.getOrElse(PrimitiveTypeSupport.DummyEnum.DummyEnumerationValue)
          enumValueTEF[A](e)
        }
      }

    // =========================== Numerical Integral ===========================

    val byteTEF: IntegralTypedExpressionFactory[Byte, TByte, Float, TFloat] with PrimitiveJdbcMapper[Byte] =
      new IntegralTypedExpressionFactory[Byte, TByte, Float, TFloat] with PrimitiveJdbcMapper[Byte] {
        val sample: Byte = 1: Byte
        val defaultColumnLength = 1
        val floatifyer: TypedExpressionFactory[Float, TFloat] = floatTEF

        def extractNativeJdbcValue(rs: ResultSet, i: Int): Byte = rs.getByte(i)
      }

    val optionByteTEF: IntegralTypedExpressionFactory[Option[Byte], TOptionByte, Option[Float], TOptionFloat]
      with DeOptionizer[Byte, Byte, TByte, Option[Byte], TOptionByte] =
      new IntegralTypedExpressionFactory[Option[Byte], TOptionByte, Option[Float], TOptionFloat]
        with DeOptionizer[Byte, Byte, TByte, Option[Byte], TOptionByte] {
        val deOptionizer: TypedExpressionFactory[Byte, TByte] with JdbcMapper[Byte, Byte] = byteTEF
        val floatifyer: TypedExpressionFactory[Option[Float], TOptionFloat] = optionFloatTEF
      }

    val intTEF: IntegralTypedExpressionFactory[Int, TInt, Float, TFloat] with PrimitiveJdbcMapper[Int] =
      new IntegralTypedExpressionFactory[Int, TInt, Float, TFloat] with PrimitiveJdbcMapper[Int] {
        val sample = 1
        val defaultColumnLength = 4
        val floatifyer: TypedExpressionFactory[Float, TFloat] = floatTEF

        def extractNativeJdbcValue(rs: ResultSet, i: Int): Int = rs.getInt(i)
      }

    val optionIntTEF: IntegralTypedExpressionFactory[Option[Int], TOptionInt, Option[Float], TOptionFloat]
      with DeOptionizer[Int, Int, TInt, Option[Int], TOptionInt] =
      new IntegralTypedExpressionFactory[Option[Int], TOptionInt, Option[Float], TOptionFloat]
        with DeOptionizer[Int, Int, TInt, Option[Int], TOptionInt] {
        val deOptionizer: TypedExpressionFactory[Int, TInt] with JdbcMapper[Int, Int] = intTEF
        val floatifyer: TypedExpressionFactory[Option[Float], TOptionFloat] = optionFloatTEF
      }

    val longTEF: IntegralTypedExpressionFactory[Long, TLong, Double, TDouble] with PrimitiveJdbcMapper[Long] =
      new IntegralTypedExpressionFactory[Long, TLong, Double, TDouble] with PrimitiveJdbcMapper[Long] {
        val sample = 1L
        val defaultColumnLength = 8
        val floatifyer: TypedExpressionFactory[Double, TDouble] = doubleTEF

        def extractNativeJdbcValue(rs: ResultSet, i: Int): Long = rs.getLong(i)
      }

    val optionLongTEF: IntegralTypedExpressionFactory[Option[Long], TOptionLong, Option[Double], TOptionDouble]
      with DeOptionizer[Long, Long, TLong, Option[Long], TOptionLong] =
      new IntegralTypedExpressionFactory[Option[Long], TOptionLong, Option[Double], TOptionDouble]
        with DeOptionizer[Long, Long, TLong, Option[Long], TOptionLong] {
        val deOptionizer: TypedExpressionFactory[Long, TLong] with JdbcMapper[Long, Long] = longTEF
        val floatifyer: TypedExpressionFactory[Option[Double], TOptionDouble] = optionDoubleTEF
      }

    // =========================== Numerical Floating Point ===========================

    val floatTEF: FloatTypedExpressionFactory[Float, TFloat] with PrimitiveJdbcMapper[Float] =
      new FloatTypedExpressionFactory[Float, TFloat] with PrimitiveJdbcMapper[Float] {
        val sample = 1f
        val defaultColumnLength = 4

        def extractNativeJdbcValue(rs: ResultSet, i: Int): Float = rs.getFloat(i)
      }

    val optionFloatTEF: FloatTypedExpressionFactory[Option[Float], TOptionFloat]
      with DeOptionizer[Float, Float, TFloat, Option[Float], TOptionFloat] =
      new FloatTypedExpressionFactory[Option[Float], TOptionFloat]
        with DeOptionizer[Float, Float, TFloat, Option[Float], TOptionFloat] {
        val deOptionizer: TypedExpressionFactory[Float, TFloat] with JdbcMapper[Float, Float] = floatTEF
      }

    val doubleTEF: FloatTypedExpressionFactory[Double, TDouble] with PrimitiveJdbcMapper[Double] =
      new FloatTypedExpressionFactory[Double, TDouble] with PrimitiveJdbcMapper[Double] {
        val sample = 1d
        val defaultColumnLength = 8

        def extractNativeJdbcValue(rs: ResultSet, i: Int): Double = rs.getDouble(i)
      }

    val optionDoubleTEF: FloatTypedExpressionFactory[Option[Double], TOptionDouble]
      with DeOptionizer[Double, Double, TDouble, Option[Double], TOptionDouble] =
      new FloatTypedExpressionFactory[Option[Double], TOptionDouble]
        with DeOptionizer[Double, Double, TDouble, Option[Double], TOptionDouble] {
        val deOptionizer: TypedExpressionFactory[Double, TDouble] with JdbcMapper[Double, Double] = doubleTEF
      }

    val bigDecimalTEF: FloatTypedExpressionFactory[BigDecimal, TBigDecimal] with PrimitiveJdbcMapper[BigDecimal] =
      new FloatTypedExpressionFactory[BigDecimal, TBigDecimal] with PrimitiveJdbcMapper[BigDecimal] {
        val sample: BigDecimal = BigDecimal(1)
        val defaultColumnLength: Int = -1

        def extractNativeJdbcValue(rs: ResultSet, i: Int): BigDecimal = {
          val v = rs.getBigDecimal(i)
          if (rs.wasNull())
            null
          else
            BigDecimal(v)
        }
      }

    val optionBigDecimalTEF: FloatTypedExpressionFactory[Option[BigDecimal], TOptionBigDecimal]
      with DeOptionizer[BigDecimal, BigDecimal, TBigDecimal, Option[BigDecimal], TOptionBigDecimal] =
      new FloatTypedExpressionFactory[Option[BigDecimal], TOptionBigDecimal]
        with DeOptionizer[BigDecimal, BigDecimal, TBigDecimal, Option[BigDecimal], TOptionBigDecimal] {
        val deOptionizer: TypedExpressionFactory[BigDecimal, TBigDecimal] with JdbcMapper[BigDecimal, BigDecimal] =
          bigDecimalTEF
      }
  }

  initialize()

  /**
   * Populates the type registry with all built-in primitive types (numerics, strings, booleans,
   * dates, timestamps, java.time types, UUIDs, byte arrays, and typed arrays), plus a special
   * entry for `Enumeration#Value`.
   *
   * Enumeration values are handled specially: they are stored as `Int` in the database, but the
   * conversion from the raw integer back to the correct `Enumeration#Value` is deferred to
   * [[FieldMetaData.canonicalEnumerationValueFor]], since the correct parent `Enumeration` is
   * only known at the field level.
   *
   * Called automatically during trait initialization. Subclasses may override to register
   * additional types, but should call `super.initialize()` first.
   *
   * @return the registry entry for `Enumeration#Value`'s superclass (an implementation detail)
   */
  protected def initialize(): Option[FieldAttributesBasedOnType[_]] = {
    import PrimitiveTypeSupport._

    register(byteTEF)
    register(intTEF)
    register(longTEF)
    register(floatTEF)
    register(doubleTEF)
    register(bigDecimalTEF)
    register(binaryTEF)
    register(booleanTEF)
    register(stringTEF)
    register(timestampTEF)
    register(localDateTimeTEF)
    register(offsetTimeTEF)
    register(offsetDateTimeTEF)
    register(dateTEF)
    register(sqlDateTEF)
    register(localDateTEF)
    register(localTimeTEF)
    register(uuidTEF)
    register(intArrayTEF)
    register(longArrayTEF)
    register(doubleArrayTEF)
    register(stringArrayTEF)

    val re: JdbcMapper[Int, Enumeration#Value]
      with TypedExpressionFactory[Enumeration#Value, TEnumValue[Enumeration#Value]] = enumValueTEF(
      DummyEnum.DummyEnumerationValue
    )

    /**
     * Enumerations are treated differently, since the map method should normally
     * return the actual Enumeration#value, but given that an enum is not only
     * determined by the int value from the DB, but also the parent Enumeration
     * parentEnumeration.values.find(_.id == v), the conversion is done
     * in FieldMetaData.canonicalEnumerationValueFor(i: Int)
     */
    val z = new FieldAttributesBasedOnType[Any](
      new MapperForReflection {
        def map(rs: ResultSet, i: Int): Any = rs.getInt(i)
        def convertToJdbc(v: AnyRef): AnyRef = v
      },
      re.defaultColumnLength,
      re.sample,
      classOf[java.lang.Integer]
    )

    registry.put(z.clasz, z)
    registry.put(z.clasz.getSuperclass, z)
  }

  /**
   * Internal bridge interface used by [[FieldAttributesBasedOnType]] to read values from a
   * `ResultSet` and convert Scala values to their JDBC representations via reflection-friendly
   * untyped signatures.
   */
  protected trait MapperForReflection {
    def map(rs: ResultSet, i: Int): Any
    def convertToJdbc(v: AnyRef): AnyRef
  }

  /**
   * Wraps a typed [[org.squeryl.dsl.JdbcMapper]] into an untyped [[MapperForReflection]],
   * erasing the type parameters so the registry can store heterogeneous mappers uniformly.
   *
   * @param fa0 the typed JDBC mapper to wrap
   * @return a [[MapperForReflection]] that delegates to `fa0`
   */
  protected def makeMapper(fa0: JdbcMapper[_, _]) = new MapperForReflection {
    val fa = fa0.asInstanceOf[JdbcMapper[AnyRef, AnyRef]]

    def map(rs: ResultSet, i: Int): AnyRef = fa.map(rs, i)

    def convertToJdbc(v: AnyRef): AnyRef = {
      if (v != null)
        fa.convertToJdbc(v)
      else null
    }
  }

  /**
   * Bundles the JDBC mapping metadata for a single registered Scala type: its mapper, default
   * column length, a sample value (used for type probing), and the native JDBC class.
   *
   * The `clasz` field is derived from the sample value's runtime class. If the sample's class
   * implements [[org.squeryl.TargetsValuesSupertype]], the ''superclass'' is used instead. This
   * mechanism allows custom value types (that extend a common supertype marker) to be registered
   * under their parent class, so that all subtypes sharing the same JDBC representation resolve
   * to a single registry entry.
   *
   * @param mapper the reflection-friendly mapper for reading/writing JDBC values
   * @param defaultLength the default column length (e.g. 128 for strings, -1 when length is driver-determined)
   * @param sample a representative sample value of type `A`
   * @param nativeJdbcType the Java class of the value as seen by JDBC (e.g. `classOf[java.lang.Integer]`)
   * @tparam A the Scala type being registered
   */
  protected class FieldAttributesBasedOnType[A](
    val mapper: MapperForReflection,
    val defaultLength: Int,
    val sample: A,
    val nativeJdbcType: Class[_]
  ) {

    val clasz: Class[_] = {
      val sampleClass = sample.asInstanceOf[AnyRef].getClass
      if (classOf[TargetsValuesSupertype].isAssignableFrom(sampleClass)) sampleClass.getSuperclass else sampleClass
    }

    override def toString: String =
      clasz.getCanonicalName + " --> " + mapper.getClass.getCanonicalName
  }

  /**
   * Converts a non-native Scala value to its JDBC representation using the registered mapper
   * for the given class.
   *
   * @param nonNativeType the runtime class of the value (used to look up the mapper)
   * @param r the value to convert
   * @return the JDBC-compatible representation of `r`
   * @throws RuntimeException if `nonNativeType` is not registered
   */
  def nativeJdbcValueFor(nonNativeType: Class[_], r: AnyRef): AnyRef =
    get(nonNativeType).mapper.convertToJdbc(r)

  /**
   * Returns `true` if the given class has a registered JDBC mapping, or if it is an `Option`
   * or `Product1` wrapper (custom types).
   *
   * @param c the class to check
   * @return whether `c` can be used as a column type in Squeryl schemas
   */
  def isSupported(c: Class[_]): Boolean =
    lookup(c).isDefined ||
      c.isAssignableFrom(classOf[Some[_]]) ||
      classOf[Product1[Any]].isAssignableFrom(c)

  /**
   * Returns the default column length for the given registered type.
   *
   * @param c the class to look up
   * @return the default column length (e.g. 128 for `String`, -1 for driver-determined types)
   * @throws RuntimeException if `c` is not registered
   */
  def defaultColumnLength(c: Class[_]): Int =
    get(c).defaultLength

  /**
   * Returns the native JDBC class that the given Scala type maps to.
   * For example, a custom `Instant` type maps to `OffsetDateTime` at the JDBC level.
   *
   * @param c the Scala class to look up
   * @return the JDBC-level class used for database operations
   * @throws RuntimeException if `c` is not registered
   */
  def nativeJdbcTypeFor(c: Class[_]): Class[_] =
    get(c).nativeJdbcType

  /**
   * Returns a function that reads a value of the given type from a `ResultSet` at a given
   * column index, returning `null` if the SQL value was NULL.
   *
   * @param c the class to look up
   * @return a `(ResultSet, Int) => AnyRef` closure suitable for result set iteration
   * @throws RuntimeException if `c` is not registered
   */
  def resultSetHandlerFor(c: Class[_]): (ResultSet, Int) => AnyRef = {
    val fa = get(c)
    (rs: ResultSet, i: Int) => {
      val z = fa.mapper.map(rs, i)
      if (rs.wasNull) null
      else z.asInstanceOf[AnyRef]
    }
  }

  private def get(c: Class[_]) =
    lookup(c).getOrElse(
      Utils.throwError(
        "Usupported native type " + c.getCanonicalName + "," + c.getName + "\n" + registry.mkString("\n")
      )
    )

  /**
   * Returns a sample (representative) value for the given registered type. This is used by
   * [[DatabaseAdapter.databaseTypeFor]] to determine the SQL column type via `instanceof` checks
   * against the sample.
   *
   * @param c the class to look up
   * @return a non-null sample value of the registered type
   * @throws RuntimeException if `c` is not registered
   */
  def sampleValueFor(c: Class[_]): AnyRef =
    get(c).sample.asInstanceOf[AnyRef]

  /**
   * Like [[sampleValueFor]], but returns `null` instead of throwing if the class is not registered.
   *
   * @param c the class to look up
   * @return a sample value, or `null` if `c` has no registration
   */
  def trySampleValueFor(c: Class[_]): AnyRef = {
    val r = lookup(c).map(_.sample)
    r match {
      case Some(x: AnyRef) => x
      case _ => null
    }
  }

  /**
   * Registers a non-primitive (composite) JDBC mapper. Non-primitive types are those that do not
   * correspond directly to a JDBC type but are instead converted to/from a primitive JDBC type
   * (e.g. `Instant` is stored as `OffsetDateTime`).
   *
   * @param m the non-primitive mapper to register
   * @throws RuntimeException if the type is already registered
   */
  private[squeryl] def register[P, A](m: NonPrimitiveJdbcMapper[P, A, _]): Unit = {

    val z =
      new FieldAttributesBasedOnType(makeMapper(m), m.defaultColumnLength, m.sample, m.primitiveMapper.nativeJdbcType)

    val wasThere = registry.put(z.clasz, z)

    if (wasThere.isDefined)
      Utils.throwError("field type " + z.clasz + " already registered, handled by " + m.getClass.getCanonicalName)
  }

  /**
   * Registers an array JDBC mapper for SQL array column types (e.g. `int[]`, `varchar[]`).
   *
   * @param m the array mapper to register
   * @throws RuntimeException if the array element type is already registered
   */
  def register[S, J](m: ArrayJdbcMapper[S, J]): Unit = {
    val f = m.thisTypedExpressionFactory
    val z = new FieldAttributesBasedOnType(makeMapper(m), m.defaultColumnLength, f.sample, m.nativeJdbcType)

    val wasThere = registry.put(z.clasz, z)

    if (wasThere.isDefined)
      Utils.throwError("field type " + z.clasz + " already registered, handled by " + m.getClass.getCanonicalName)
  }

  private def register[A](pm: PrimitiveJdbcMapper[A]): Unit = {
    val f = pm.thisTypedExpressionFactory
    val z = new FieldAttributesBasedOnType(makeMapper(pm), f.defaultColumnLength, f.sample, pm.nativeJdbcType)

    val c = z.clasz

    registry.put(c, z)
  }

  @tailrec
  private def lookup(c: Class[_]): Option[FieldAttributesBasedOnType[_]] = {
    if (!c.isPrimitive)
      registry.get(c)
    else
      c.getName match {
        case "int" => lookup(classOf[java.lang.Integer])
        case "long" => lookup(classOf[java.lang.Long])
        case "float" => lookup(classOf[java.lang.Float])
        case "byte" => lookup(classOf[java.lang.Byte])
        case "boolean" => lookup(classOf[java.lang.Boolean])
        case "double" => lookup(classOf[java.lang.Double])
        case "void" => None
      }
  }
}
