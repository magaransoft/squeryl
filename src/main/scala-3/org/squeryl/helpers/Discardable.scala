package org.squeryl.helpers

/** An opaque type wrapper that marks mutation return values as safely discardable.
  *
  * Squeryl mutation methods (e.g., `Table.insert`, `Table.update`) return values that callers
  * frequently ignore. Under strict compiler flags (`-Wnonunit-statement` and `-Wvalue-discard`),
  * discarding these return values produces warnings. Wrapping the return type in `Discardable`
  * suppresses these warnings because the compiler sees that the value is explicitly allowed to be
  * discarded via the implicit `Conversion`.
  *
  * The upper bound `<: A` makes the opaque type transparent to callers who do capture the return
  * value: a `Discardable[T]` is a subtype of `T`, so it can be assigned to a `T` variable or
  * passed where a `T` is expected without any explicit unwrapping.
  *
  * @tparam A the underlying return type being wrapped
  */
opaque type Discardable[A] <: A = A

/** Companion object for [[Discardable]], providing construction and implicit conversion. */
object Discardable {

  /** Implicit conversion that allows any `A` to be silently promoted to `Discardable[A]`.
    *
    * This is what enables mutation methods to return `Discardable[T]` while the caller
    * discards the result without triggering `-Wvalue-discard` warnings. The Scala 3 compiler
    * recognizes that a `Conversion` to the return type exists and treats the discard as intentional.
    */
  given [A]: Conversion[A, Discardable[A]] = (x: A) => x

  /** Wraps a value of type `A` into a `Discardable[A]`.
    *
    * @param x the value to wrap
    * @return the same value, typed as `Discardable[A]`
    */
  def apply[A](x: A): Discardable[A] = x
}
