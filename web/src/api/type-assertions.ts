/**
 * Type-level assertion helpers. `Expect<Equal<A, B>>` fails to compile when
 * the two types are not identical (mutual assignability is not enough).
 * Adapted from the widely used idiom popularized in the TypeScript community.
 */
export type Equal<X, Y> =
  (<T>() => T extends X ? 1 : 2) extends <T>() => T extends Y ? 1 : 2 ? true : false;

export type Expect<T extends true> = T;
