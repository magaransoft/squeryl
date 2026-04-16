package org.squeryl.internals

import scala.quoted.Expr
import scala.quoted.Quotes
import scala.quoted.Type

/** Compile-time macro utility that extracts type information from `Option` fields of a case class or trait.
  *
  * In Scala 2, Squeryl uses `ScalaSig` (the Scala signature stored in class file annotations) to
  * inspect `Option` fields and determine their contained primitive type at runtime. Scala 3 no longer
  * emits `ScalaSig` — type parameters of `Option` fields are erased and invisible at runtime. This
  * macro bridges that gap by inspecting the full type information at compile time and producing a
  * `Map` that is available at runtime.
  *
  * The resulting map is consumed by `FieldMetaData` during table initialization to correctly identify
  * the JDBC type of `Option[Int]`, `Option[Long]`, and other `Option`-wrapped primitive fields that
  * would otherwise be indistinguishable from `Option[Object]` after erasure.
  */
object TypeInfo {

  /** Inline entry point that triggers compile-time macro expansion of [[fieldsInfo]].
    *
    * @tparam T the entity type whose `Option` fields should be inspected
    * @return a `Map` from field name to the boxed `Class` of the primitive type contained in each
    *         `Option` field. Fields that are not `Option` or contain non-primitive types are excluded.
    */
  inline def fieldsInfo[T <: AnyKind]: Map[String, Class[_]] = ${ fieldsInfo[T] }

  /** Macro implementation that inspects `T` at compile time and builds an expression producing
    * a `Map[String, Class[_]]` of Option-wrapped primitive field metadata.
    *
    * For each `val` field of `T` whose type is `Option[P]` where `P` is a JVM primitive type
    * (`Int`, `Short`, `Long`, `Double`, `Float`, `Boolean`, `Byte`, or `Char`), the macro emits
    * an entry mapping the field name to `classOf[P]`. Non-`val` members, non-`Option` fields,
    * and `Option` fields containing non-primitive types (e.g., `Option[String]`) are excluded.
    *
    * @tparam T the entity type to inspect
    * @param qctx0 the implicit `Quotes` context provided by the Scala 3 macro system
    * @return a quoted expression evaluating to `Map[String, Class[_]]`
    */
  def fieldsInfo[T <: AnyKind: Type](using qctx0: Quotes): Expr[Map[String, Class[_]]] = {
    import qctx0.reflect.*

    val uns = TypeTree.of[T]
    val symbol = uns.symbol
    val innerClassOfOptionFields: Map[String, Class[_]] = symbol.fieldMembers.flatMap { m =>
      // we only support val fields for now
      if (m.isValDef) {
        val tpe = ValDef(m, None).tpt.tpe
        // only if the field is an Option[_]
        if (tpe.typeSymbol == TypeRepr.of[Option[_]].typeSymbol) {
          val containedClass: Option[Class[_]] = PartialFunction.condOpt(tpe.asType) {
            case '[Option[Int]] =>
              classOf[Int]
            case '[Option[Short]] =>
              classOf[Short]
            case '[Option[Long]] =>
              classOf[Long]
            case '[Option[Double]] =>
              classOf[Double]
            case '[Option[Float]] =>
              classOf[Float]
            case '[Option[Boolean]] =>
              classOf[Boolean]
            case '[Option[Byte]] =>
              classOf[Byte]
            case '[Option[Char]] =>
              classOf[Char]
          }

          containedClass.map(clazz => (m.name -> clazz))
        } else None
      } else None
    }.toMap

    Expr(innerClassOfOptionFields)
  }

}
