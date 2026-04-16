# Squeryl [![Tests & Docs](https://github.com/magaransoft/squeryl/actions/workflows/ci.yml/badge.svg)](https://github.com/magaransoft/squeryl/actions/workflows/ci.yml)

A Scala 3 ORM and DSL for talking with databases using minimum verbosity and maximum type safety.

This is a fork of the original [squeryl/squeryl](https://github.com/squeryl/squeryl), maintained by [Magaran Software LLC](https://github.com/magaransoft). See the [NOTICE](NOTICE) file for full attribution.

## Installation

Squeryl is published to Maven Central for Scala 3. Add the following to your `build.sbt`:

```scala
libraryDependencies += "com.magaran" %% "squeryl" % "0.1.0"
```

## Changes from upstream

This fork targets **Scala 3 only** and adds several features on top of the original Squeryl. The core query DSL, database adapters, schema/table DDL generation, and `CustomType` mechanism are all unchanged from upstream.

### Java `time` API support

The main feature addition. Upstream only supports `java.util.Date`, `java.sql.Date`, and `java.sql.Timestamp`. This fork adds full first-class support for the modern `java.time` API:

| Type | SQL mapping | Notes |
|---|---|---|
| `LocalDate` | `date` | Via JDBC 4.2 `getObject` |
| `LocalTime` | `time` | Via JDBC 4.2 `getObject` |
| `LocalDateTime` | `timestamp` | Via JDBC 4.2 `getObject` |
| `OffsetTime` | `time` | Via JDBC 4.2 `getObject` |
| `OffsetDateTime` | `timestamp` | Via JDBC 4.2 `getObject` |
| `Instant` | `timestamp` | Stored as UTC `OffsetDateTime` |
| `ZonedDateTime` | `timestamp` | Implicit conversion to `Instant` (stores as UTC) |

All types support `Option[T]` variants. No user-side configuration is needed — declare the field type in your model and the mapping is automatic, the same way `Timestamp` has always worked:

```scala
import org.squeryl.PrimitiveTypeMode.*

class Event(
  val id: Long,
  val name: String,
  val scheduledAt: LocalDateTime,
  val createdAt: Instant
) extends KeyedEntity[Long]
```

### `Discardable[A]` return types

Insert, update, and delete operations now return `Discardable[T]` — a Scala 3 opaque type that is erased at runtime (`opaque type Discardable[A] <: A = A`). This enables compiler warnings (`-Wnonunit-statement`, `-Wvalue-discard`) when return values from side-effecting operations are silently discarded, catching a common source of bugs:

```scala
// Compiler warns: discarded non-Unit value of type Discardable[Int]
table.deleteWhere(t => t.id === 5)

// Explicit discard or assignment silences the warning
val _ = table.deleteWhere(t => t.id === 5)
val count = table.deleteWhere(t => t.id === 5) // count: Int (transparent)
```

A `given Conversion[A, Discardable[A]]` is provided, so existing code that captures return values continues to compile without changes.

### `TargetsValuesSupertype` marker trait

When a model field's value implements `TargetsValuesSupertype`, Squeryl looks up JDBC mappers for the value's superclass rather than the concrete class. This enables `NonPrimitiveJdbcMapper` for types where the sample value is a concrete subtype but the field declaration uses the supertype.

### Scala 3 compile-time field introspection

The `macros` subproject provides `inline def fieldsInfo[T]` using Scala 3 `scala.quoted` to introspect case class fields at compile time. This feeds precise type information into `FieldMetaData` for Scala 3 case classes, where erasure would otherwise lose the distinction between primitive and boxed types in `Option[Int]`, `Option[Long]`, etc.

### cglib replaced with ByteBuddy

The proxy generation used for field reference linking (originally `cglib-nodep`) was replaced with `net.bytebuddy:byte-buddy`. This was an upstream change absorbed into this fork.

### Minor fixes

- Array type detection fix in `ArrayTEF`: uses `toWrappedJDBCType(sample(0)).getClass` instead of `sample(0).getClass`, fixing type detection when the element wrapper differs from the element type.
- `CanCompare` evidence added for `TIntArray`/`TLongArray` comparisons (upstream omission).

## License

This project is licensed under the [Apache License 2.0](license.txt).
