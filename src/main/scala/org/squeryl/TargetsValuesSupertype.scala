package org.squeryl

/** Marker trait that instructs Squeryl's JDBC mapper lookup to resolve mappers using the
  * '''direct superclass''' of the marked type rather than the type itself.
  *
  * When Squeryl encounters a field whose runtime type extends `TargetsValuesSupertype`, the
  * `FieldMetaData` and `FieldMapper` subsystems call `getClass.getSuperclass` on the value
  * and use the resulting parent class to find the registered JDBC mapper. Without this marker,
  * Squeryl would look for a mapper registered against the concrete (sub)class, which typically
  * has no mapper registered and would cause a lookup failure.
  *
  * ==When to use==
  *
  * Use this trait when you define a custom type that extends a base type for which a JDBC mapper
  * is already registered, and you want Squeryl to use the base type's mapper rather than
  * requiring a separate mapper for your subtype.
  *
  * ==Example==
  *
  * Suppose you have a `BigDecimal`-backed mapper registered for a `Money` base class, and you
  * want to define a `Price` subclass that reuses the same mapper:
  *
  * {{{
  * // Base type with a registered JDBC mapper
  * class Money(val amount: BigDecimal)
  *
  * // Custom subtype — extends TargetsValuesSupertype so Squeryl resolves the
  * // mapper for Money (the superclass) instead of looking for a Price mapper
  * class Price(amount: BigDecimal) extends Money(amount) with TargetsValuesSupertype
  *
  * // In your entity
  * case class Product(id: Long, unitPrice: Price)
  *
  * // Squeryl sees that Price extends TargetsValuesSupertype, so it calls
  * // classOf[Price].getSuperclass (which is Money) and uses Money's mapper
  * // to read/write the unitPrice column.
  * }}}
  *
  * This also works for `Option` fields: if a field is typed as `Option[Price]`, Squeryl
  * unwraps the `Some` value, detects the `TargetsValuesSupertype` marker, and resolves the
  * mapper via the superclass in the same way.
  */
trait TargetsValuesSupertype {}
