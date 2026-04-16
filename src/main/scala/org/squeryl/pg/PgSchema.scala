package org.squeryl.pg

import org.squeryl._
import internals.{StatementWriter, FieldMapper}
import dsl.ast.{ViewExpressionNode, ExpressionNode}
import reflect.ClassTag

/**
 * PostgreSQL-specific schema extension that adds support for set-returning functions (SRFs).
 * SRFs are PostgreSQL functions that return a set of rows (e.g., `generate_series`,
 * `unnest`, or custom PL/pgSQL functions returning `SETOF`).
 *
 * Use the `srf` methods to define a set-returning function that can be queried like a
 * regular Squeryl view/table. The function arguments are passed as expression nodes and
 * rendered inline in the generated SQL (e.g., `SELECT * FROM my_function(arg1, arg2)`).
 */
class PgSchema(implicit fieldMapper: FieldMapper) extends Schema()(fieldMapper) {

  /**
   * Defines a set-returning function using the class name of `T` as the function name.
   *
   * @tparam T the row type returned by the function
   * @return a function that takes expression arguments and returns a queryable [[View]]
   */
  protected def srf[T]()(implicit man: ClassTag[T]): (Seq[ExpressionNode] => View[T]) =
    srf(tableNameFromClass(man.runtimeClass))(man)

  /**
   * Defines a set-returning function with an explicit name.
   *
   * @param name the SQL function name
   * @tparam T the row type returned by the function
   * @return a function that takes expression arguments and returns a queryable [[View]]
   */
  protected def srf[T](name: String)(implicit man: ClassTag[T]): (Seq[ExpressionNode] => View[T]) =
    srf0(name, None, _: _*)

  /**
   * Defines a set-returning function with an explicit name and a schema/table prefix.
   *
   * @param name   the SQL function name
   * @param prefix the schema prefix prepended to the function name in SQL
   * @tparam T the row type returned by the function
   * @return a function that takes expression arguments and returns a queryable [[View]]
   */
  protected def srf[T](name: String, prefix: String)(implicit man: ClassTag[T]): (Seq[ExpressionNode] => View[T]) =
    srf0(name, Some(prefix), _: _*)

  private def srf0[T](name: String, prefix: Option[String], args: ExpressionNode*)(implicit
    man: ClassTag[T]
  ): View[T] = {
    val typeT = man.runtimeClass.asInstanceOf[Class[T]]
    new SrfView[T](name, typeT, this, prefix, args)
  }
}

/**
 * A [[View]] implementation that represents a PostgreSQL set-returning function.
 * When used in a query, it renders as `function_name(args...)` instead of a plain table name.
 *
 * @param name     the SQL function name
 * @param classOfT the runtime class of the row type
 * @param schema   the schema this view belongs to
 * @param prefix   optional schema prefix for the function name
 * @param args     the expression nodes to pass as function arguments
 * @tparam T the row type returned by the function
 */
class SrfView[T](
  name: String,
  classOfT: Class[T],
  schema: Schema,
  prefix: Option[String],
  args: Iterable[ExpressionNode]
) extends View[T](name, classOfT, schema, prefix, None, None) {
  override def viewExpressionNode: ViewExpressionNode[T] = new SrfViewExpressionNode[T](this, args)
}

/**
 * Expression node that renders a set-returning function call in the FROM clause.
 * Produces SQL like `"function_name"(arg1, arg2, ...)` by writing the quoted function
 * name followed by the argument expressions in parentheses.
 *
 * @param view the [[SrfView]] this node represents
 * @param args the expression nodes to render as function arguments
 * @tparam T the row type returned by the function
 */
class SrfViewExpressionNode[T](view: View[T], args: Iterable[ExpressionNode]) extends ViewExpressionNode(view) {
  override def doWrite(sw: StatementWriter) = {
    sw.write(sw.quoteName(view.prefixedName))
    sw.write("(")
    sw.writeNodesWithSeparator(args, ",", false)
    sw.write(")")
  }
}
