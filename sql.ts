import { inspect } from "bun";

/**
 * A SQL query fragment that can be composed with other fragments to build
 * complex queries. Implements PromiseLike so it can be awaited directly to
 * execute the query.
 *
 * @template T The expected return type when the query is executed
 */
export class SQLFragment<T = unknown> implements PromiseLike<Array<T>> {
  public readonly sql: Bun.SQL;
  public readonly parts: Array<string>;
  public readonly params: Array<SQLScalar>;

  constructor(sql: Bun.SQL) {
    this.sql = sql;
    this.parts = [""];
    this.params = [];
  }

  /**
   * Constructs the SQL query string with parameter placeholders.
   *
   * @returns The complete SQL query string with parameter placeholders
   *
   * @example
   * const fragment = sql`SELECT * FROM users WHERE id = ${123} AND name = ${'Alice'}`
   * fragment.query; // returns: "SELECT * FROM users WHERE id = $1 AND name = $2"
   * fragment.params; // returns: [123, 'Alice']
   * ```
   */
  get query(): string {
    let query = String(this.parts[0]);
    for (let i = 1; i < this.parts.length; i++) {
      query += `$${i}`;
      query += this.parts[i];
    }
    return query;
  }

  /**
   * Converts the fragment to a Bun SQL query. You might find it easier to
   * directly `await` the SQLFragment.
   */
  toQuery(): Bun.SQL.Query<Array<T>> {
    return this.sql.unsafe<Array<T>>(this.query, this.params);
  }

  /**
   * Executes the query. Returns a running Query object that can e.g., be
   * canceled. If you don't need the Query object, you might find it easier to
   * directly `await` the SQLFragment
   */
  execute(): Bun.SQL.Query<Array<T>> {
    return this.toQuery().execute();
  }

  /**
   * Executes the query, returning the result
   */
  then<TResult1 = Array<T>, TResult2 = never>(
    onfulfilled?:
      | ((value: Array<T>) => TResult1 | PromiseLike<TResult1>)
      | undefined
      | null,
    onrejected?:
      | ((reason: unknown) => TResult2 | PromiseLike<TResult2>)
      | undefined
      | null,
  ): PromiseLike<TResult1 | TResult2> {
    return this.toQuery().then(onfulfilled, onrejected);
  }

  [inspect.custom]() {
    // There's a lot of crap on the actual class that isn't helpful when
    // debugging
    class SQLFragment {
      constructor(
        public query: string,
        public params: Array<SQLScalar>,
      ) {}
    }
    return new SQLFragment(this.query, this.params);
  }
}

/**
 * Types that can be used as SQL parameters - basic scalar values that can be safely
 * passed to PostgreSQL.
 */
export type SQLScalar =
  | null
  | boolean
  | number
  | bigint
  | string
  | Date
  | Buffer
  | ArrayBuffer
  | ArrayBufferView;

/**
 * SQL primitive types including both scalars and arrays of scalars.
 * Arrays are converted to PostgreSQL ARRAY[] syntax.
 */
export type SQLPrimitive = SQLScalar | Array<SQLPrimitive>;

// These pushFoo functions help construct fragments

function pushPart(frag: SQLFragment, part: string) {
  frag.parts[frag.parts.length - 1] += part;
}
function pushParam(frag: SQLFragment, param: SQLScalar) {
  frag.parts.push("");
  frag.params.push(param);
}

function pushValue(
  frag: SQLFragment,
  value: SQLPrimitive,
  inArray: boolean = false,
) {
  if (Array.isArray(value)) {
    pushPart(frag, inArray ? "[" : "ARRAY[");
    for (let i = 0; i < value.length; i++) {
      pushValue(frag, value[i]!, true);
      if (i < value.length - 1) {
        pushPart(frag, ", ");
      }
    }
    pushPart(frag, "]");
  } else {
    pushParam(frag, value);
  }
}

function pushFragment(frag: SQLFragment, other: SQLFragment) {
  for (let j = 0; j < other.parts.length; j++) {
    pushPart(frag, other.parts[j]!);
    if (j < other.parts.length - 1) {
      pushParam(frag, other.params[j]!);
    }
  }
}

// https://www.postgresql.org/docs/current/sql-keywords-appendix.html
// List of reserved keywords, which cannot be used as table or column names
// without being quoted
const reservedKeywords = new Set([
  "all",
  "analyse",
  "analyze",
  "and",
  "any",
  "array",
  "as",
  "asc",
  "asymmetric",
  "authorization",
  "binary",
  "both",
  "case",
  "cast",
  "check",
  "collate",
  "collation",
  "column",
  "concurrently",
  "constraint",
  "create",
  "cross",
  "current_catalog",
  "current_date",
  "current_role",
  "current_schema",
  "current_time",
  "current_timestamp",
  "current_user",
  "default",
  "deferrable",
  "desc",
  "distinct",
  "do",
  "else",
  "end",
  "except",
  "false",
  "fetch",
  "for",
  "foreign",
  "freeze",
  "from",
  "full",
  "grant",
  "group",
  "having",
  "ilike",
  "in",
  "initially",
  "inner",
  "intersect",
  "into",
  "is",
  "isnull",
  "join",
  "lateral",
  "leading",
  "left",
  "like",
  "limit",
  "localtime",
  "localtimestamp",
  "natural",
  "not",
  "notnull",
  "null",
  "offset",
  "on",
  "only",
  "or",
  "order",
  "outer",
  "overlaps",
  "placing",
  "primary",
  "references",
  "returning",
  "right",
  "select",
  "session_user",
  "similar",
  "some",
  "symmetric",
  "system_user",
  "table",
  "tablesample",
  "then",
  "to",
  "trailing",
  "true",
  "union",
  "unique",
  "user",
  "using",
  "variadic",
  "verbose",
  "when",
  "where",
  "window",
  "with",
]);

function escapeID(s: string): string {
  if (
    s.length > 0 &&
    /^[a-z_][a-z0-9$_]*$/.test(s) &&
    !reservedKeywords.has(s)
  ) {
    return s;
  }

  let needsUnicode = false;
  for (let i = 0; i < s.length; i++) {
    const code = s.charCodeAt(i);
    if (code > 127 || code < 32 || code === 127) {
      needsUnicode = true;
      break;
    }
  }

  if (!needsUnicode) {
    return '"' + s.replace(/"/g, '""') + '"';
  }

  let result = 'U&"';
  for (const char of s) {
    if (char === '"') {
      result += '""';
    } else {
      const code = char.codePointAt(0)!;
      if (code > 127 || code < 32 || code === 127) {
        result += "\\" + code.toString(16).padStart(code > 0xffff ? 6 : 4, "0");
      } else {
        result += char;
      }
    }
  }
  result += '"';

  return result;
}

function id(sql: Bun.SQL, s: string): SQLFragment<never> {
  const frag = new SQLFragment<never>(sql);
  pushPart(frag, escapeID(s));
  return frag;
}

function unsafe<T>(sql: Bun.SQL, s: string): SQLFragment<T> {
  const frag = new SQLFragment<T>(sql);
  pushPart(frag, s);
  return frag;
}

function valuesObjInner(
  frag: SQLFragment<never>,
  values: Array<Record<string, SQLPrimitive>>,
  cols: Array<string>,
): SQLFragment<never> {
  pushPart(frag, "VALUES (");
  for (let i = 0; i < values.length; i++) {
    const value = values[i]!;
    for (let j = 0; j < cols.length; j++) {
      const col = cols[j]!;
      if (!(col in value)) {
        throw new Error(
          `Object ${JSON.stringify(value)} missing column ${col}`,
        );
      }
      pushValue(frag, value[col]!);
      if (j !== cols.length - 1) {
        pushPart(frag, ", ");
      }
    }
    pushPart(frag, i === values.length - 1 ? ")" : "), (");
  }
  return frag;
}

function colsInner(frag: SQLFragment<never>, cols: Array<string>) {
  pushPart(frag, "(");
  for (let i = 0; i < cols.length; i++) {
    pushPart(frag, escapeID(cols[i]!));
    if (i !== cols.length - 1) {
      pushPart(frag, ", ");
    }
  }
  pushPart(frag, ")");
}

// Returns `VALUES (...), ...`
function values(
  sql: Bun.SQL,
  values: Array<Array<SQLPrimitive> | Record<string, SQLPrimitive>>,
  cols: Array<string>,
): SQLFragment<never> {
  if (values.length === 0) {
    throw new Error("VALUES must have at least one element");
  }

  if (Array.isArray(values[0])) {
    const frag = new SQLFragment<never>(sql);
    pushPart(frag, "VALUES (");
    for (let i = 0; i < values.length; i++) {
      const inner = values[i] as Array<SQLPrimitive>;
      for (let j = 0; j < inner.length; j++) {
        pushValue(frag, inner[j]!);
        if (j < inner.length - 1) {
          pushPart(frag, ", ");
        }
      }
      pushPart(frag, i === values.length - 1 ? ")" : "), (");
    }
    return frag;
  } else {
    if (cols.length === 0) {
      throw new Error("VALUES must have at least one column");
    }
    const frag = new SQLFragment<never>(sql);
    valuesObjInner(frag, values as Array<Record<string, SQLPrimitive>>, cols);
    return frag;
  }
}

// Returns `(VALUES (...), ...) AS table (col, ...)`
function valuesT(
  table: string,
  sql: Bun.SQL,
  values: Array<Record<string, SQLPrimitive>>,
  cols: Array<string>,
): SQLFragment<never> {
  if (values.length === 0) {
    throw new Error("VALUES must have at least one element");
  }

  if (cols.length === 0) {
    cols = Object.keys(values[0]!);
  }
  if (cols.length === 0) {
    throw new Error("VALUES must have at least one column");
  }

  const frag = new SQLFragment<never>(sql);
  valuesObjInner(frag, values, cols);
  pushPart(frag, ` AS ${escapeID(table)}`);
  colsInner(frag, cols);
  return frag;
}

// Returns `(col, ...) VALUES (...), ...`, as you'd use in INSERT INTO ...
function insertValues(
  sql: Bun.SQL,
  values: Array<Record<string, SQLPrimitive>>,
  cols: Array<string>,
): SQLFragment<never> {
  if (values.length === 0) {
    throw new Error("VALUES must have at least one element");
  }

  if (cols.length === 0) {
    cols = Object.keys(values[0]!);
  }
  if (cols.length === 0) {
    throw new Error("VALUES must have at least one column");
  }

  const frag = new SQLFragment<never>(sql);
  colsInner(frag, cols);
  pushPart(frag, " ");
  valuesObjInner(frag, values, cols);
  return frag;
}

function list<K extends string>(
  sql: Bun.SQL,
  values: Array<SQLPrimitive> | Array<Record<K, SQLPrimitive>>,
  key?: K,
): SQLFragment<never> {
  if (values.length === 0) {
    throw new Error("Value lists must have at least one element");
  }

  const frag = new SQLFragment<never>(sql);
  pushPart(frag, "(");

  if (key !== undefined) {
    const objValues = values as Array<Record<K, SQLPrimitive>>;
    for (let i = 0; i < objValues.length; i++) {
      pushValue(frag, objValues[i]![key]);
      if (i < objValues.length - 1) {
        pushPart(frag, ", ");
      }
    }
  } else {
    const primitiveValues = values as Array<SQLPrimitive>;
    for (let i = 0; i < primitiveValues.length; i++) {
      pushValue(frag, primitiveValues[i]!);
      if (i < primitiveValues.length - 1) {
        pushPart(frag, ", ");
      }
    }
  }

  pushPart(frag, ")");
  return frag;
}

const fragMap = {
  AND: " AND ",
  OR: " OR ",
  ",": ", ",
} as const;

function join(
  sql: Bun.SQL,
  sep: "AND" | "OR" | ",",
  frags: Array<SQLFragment>,
): SQLFragment<never> {
  const frag = new SQLFragment<never>(sql);
  for (let i = 0; i < frags.length; i++) {
    pushFragment(frag, frags[i]!);
    if (i < frags.length - 1) {
      pushPart(frag, fragMap[sep]);
    }
  }
  return frag;
}

function sqlTemplate<T>(
  sql: Bun.SQL,
  strings: TemplateStringsArray,
  values: Array<SQLFragment | Array<SQLFragment> | SQLPrimitive>,
): SQLFragment<T> {
  const frag = new SQLFragment<T>(sql);
  for (let i = 0; i < strings.length; i++) {
    pushPart(frag, strings[i]!);
    if (i < strings.length - 1) {
      const value = values[i];
      if (value instanceof SQLFragment) {
        pushFragment(frag, value);
      } else if (
        Array.isArray(value) &&
        value.length > 0 &&
        value[0] instanceof SQLFragment
      ) {
        for (const other of value) {
          if (other instanceof SQLFragment) {
            pushFragment(frag, other);
          } else {
            throw new Error(
              `Expected array of SQLFragment, but had other values mixed in: ${other}`,
            );
          }
        }
      } else {
        pushValue(frag, value as SQLPrimitive);
      }
    }
  }
  return frag;
}

async function transaction<T>(
  sql: Bun.SQL,
  fn: (tx: TransactionSQL) => Promise<T>,
): Promise<T> {
  const conn = await sql.reserve();
  const tx = <T>(
    strings: TemplateStringsArray,
    ...values: Array<SQLFragment | Array<SQLFragment> | SQLPrimitive>
  ) => sqlTemplate<T>(conn, strings, values);
  tx.inTransaction = true as const;
  tx.id = (s: string) => id(conn, s);
  tx.unsafe = <T>(sql: string) => unsafe<T>(conn, sql);
  tx.values = (
    vs: Array<Array<SQLPrimitive> | Record<string, SQLPrimitive>>,
    ...cols: Array<string>
  ) => values(conn, vs, cols);
  tx.valuesT = (
    table: string,
    vs: Array<Record<string, SQLPrimitive>>,
    ...cols: Array<string>
  ) => valuesT(table, conn, vs, cols);
  tx.insertValues = (
    vs: Array<Record<string, SQLPrimitive>>,
    ...cols: Array<string>
  ) => insertValues(conn, vs, cols);
  tx.join = (sep: "AND" | "OR" | ",", frags: Array<SQLFragment>) =>
    join(conn, sep, frags);
  tx.list = (
    values: Array<SQLPrimitive> | Array<Record<string, SQLPrimitive>>,
    key?: string,
  ) => list(conn, values, key);

  try {
    await conn.unsafe("BEGIN");
    const out = await fn(tx);
    await conn.unsafe("COMMIT");
    return out;
  } catch (e) {
    await conn.unsafe("ROLLBACK");
    throw e;
  } finally {
    conn.release();
  }
}

/**
 * Base SQL interface providing core query building functionality.
 * Used by both regular SQL operations and transaction SQL operations.
 *
 * This interface provides safe SQL query construction with automatic parameterization
 * to prevent SQL injection attacks. All values are properly escaped and typed.
 */
export interface AnySQL {
  /**
   * Template string for building SQL queries with automatic parameterization.
   * Values are automatically parameterized to prevent SQL injection.
   *
   * @template T - The expected return type when the query is executed
   * @param strings - Template string literals
   * @param values - Values to be parameterized in the query (SQLFragment or SQLPrimitive)
   * @returns SQLFragment that can be awaited to execute the query
   *
   * @example
   * // Basic query with parameters
   * const users = await sql<{id: number, name: string}>`
   *   SELECT id, name FROM users WHERE age > ${minAge} AND status = ${'active'}
   * `;
   *
   * @example
   * // Composing fragments
   * const whereClause = sql`WHERE age > ${minAge}`;
   * const users = await sql<{id: number}>`SELECT id FROM users ${whereClause}`;
   *
   * @example
   * // Complex query with multiple parameters
   * const result = await sql<{count: bigint}>`
   *   SELECT COUNT(*) as count
   *   FROM photos p
   *   LEFT JOIN albums a ON p.album_id = a.id
   *   WHERE p.account_id = ${accountID}
   *   AND p.created_at > ${startDate}
   *   AND a.name = ${albumName}
   * `;
   */
  <T = unknown>(
    strings: TemplateStringsArray,
    ...values: Array<SQLFragment | Array<SQLFragment> | SQLPrimitive>
  ): SQLFragment<T>;

  /**
   * Safely escape SQL identifiers (table names, column names, etc.).
   * Handles PostgreSQL reserved keywords and special characters.
   *
   * @param s - The identifier to escape
   * @returns SQLFragment containing the properly escaped identifier
   *
   * @example
   * // Safe table/column names
   * const tableName = 'user_data';
   * const query = sql`SELECT * FROM ${sql.id(tableName)}`;
   *
   * @example
   * // Handles reserved keywords
   * const columnName = 'order'; // PostgreSQL reserved keyword
   * const query = sql`SELECT ${sql.id(columnName)} FROM products`;
   * // Generates: SELECT "order" FROM products
   *
   * @example
   * // Dynamic column selection
   * const columns = ['id', 'name', 'created_at'];
   * const columnList = columns.map(col => sql.id(col));
   * const query = sql`SELECT ${columnList} FROM users`;
   */
  id(s: string): SQLFragment<never>;

  /**
   * Create an unsafe SQL fragment from a raw string.
   * ⚠️ WARNING: This bypasses parameterization - only use with trusted input!
   *
   * @template T - The expected return type when executed
   * @param sql - Raw SQL string (not parameterized)
   * @returns SQLFragment containing the raw SQL
   *
   * @example
   * // Dynamic ORDER BY clause (safe because it's from predefined options)
   * const validSortColumns = ['name', 'created_at', 'size'];
   * const sortBy = validSortColumns.includes(userInput) ? userInput : 'created_at';
   * const query = sql`
   *   SELECT * FROM photos
   *   WHERE account_id = ${accountID}
   *   ${sql.unsafe(`ORDER BY ${sortBy} DESC`)}
   * `;
   */
  unsafe<T = unknown>(sql: string): SQLFragment<T>;

  /**
   * Generate VALUES clause from arrays of primitives.
   *
   * @param v - Array of arrays containing values for each row
   * @returns SQLFragment containing VALUES clause
   *
   * @example
   * // Simple values with arrays
   * const data = [[1, 'Alice'], [2, 'Bob'], [3, 'Charlie']];
   * const query = sql`
   *   INSERT INTO users (id, name)
   *   ${sql.values(data)}
   * `;
   * // Generates: INSERT INTO users (id, name) VALUES ($1, $2), ($3, $4), ($5, $6)
   */
  values(v: Array<Array<SQLPrimitive>>): SQLFragment<never>;

  /**
   * Generate VALUES clause from objects with optional column specification.
   *
   * @param v - Array of objects containing row data
   * @param cols - Array of column names to include. Because VALUES is tabular,
   *   you must explicitly tell the library how to order the columns for your
   *   use case. For a more automatic experience, try calling `valuesT` or
   *   `insertValues` which optimize for two common use cases.
   * @returns SQLFragment containing VALUES clause
   *
   * @example
   * // Values from objects (auto-detect columns)
   * const users = [
   *   { id: 1, name: 'Alice', email: 'alice@example.com' },
   *   { id: 2, name: 'Bob', email: 'bob@example.com' }
   * ];
   * const query = sql`
   *   INSERT INTO users (id, name, email)
   *   ${sql.values(users)}
   * `;
   *
   * @example
   * // Values with specific columns
   * const users = [
   *   { id: 1, name: 'Alice', email: 'alice@example.com', internal_field: 'ignore' },
   *   { id: 2, name: 'Bob', email: 'bob@example.com', internal_field: 'ignore' }
   * ];
   * const query = sql`
   *   INSERT INTO users (id, name, email)
   *   ${sql.values(users, 'id', 'name', 'email')}
   * `;
   */
  values(
    v: Array<Record<string, SQLPrimitive>>,
    ...cols: [string, ...Array<string>]
  ): SQLFragment<never>;

  /**
   * Generate VALUES clause with table alias for use in complex queries.
   * Creates a temporary table that can be joined against.
   *
   * @param table - The name of the table to define
   * @param v - Array of objects containing row data
   * @param cols - Optional array of column names (defaults to Object.keys of first object)
   * @returns SQLFragment containing VALUES clause with table alias
   *
   * @example
   * // Using VALUES as a temporary table in JOINs
   * const updates = [
   *   { id: 1, new_status: 'active' },
   *   { id: 2, new_status: 'inactive' },
   *   { id: 3, new_status: 'pending' }
   * ];
   * const query = sql`
   *   UPDATE users
   *   SET status = t.new_status
   *   FROM ${sql.valuesT('t', updates)}
   *   WHERE users.id = t.id
   * `;
   * // Generates: FROM (VALUES ($1, $2), ($3, $4), ($5, $6)) AS t(id, new_status)
   *
   * @example
   * // Complex query with VALUES table
   * const photoData = [
   *   { photo_id: 1, priority: 10 },
   *   { photo_id: 2, priority: 20 }
   * ];
   * const query = sql`
   *   SELECT p.*, t.priority
   *   FROM photos p
   *   LEFT JOIN ${sql.valuesT('t', photoData)} ON p.id = t.photo_id
   *   WHERE p.account_id = ${accountID}
   * `;
   */
  valuesT(
    table: string,
    v: Array<Record<string, SQLPrimitive>>,
    ...cols: Array<string>
  ): SQLFragment<never>;

  /**
   * Generate column list and VALUES clause for INSERT statements.
   * Convenience method that combines column specification with values.
   *
   * @param v - Array of objects containing row data
   * @param cols - Optional array of column names (defaults to Object.keys of first object)
   * @returns SQLFragment containing (columns) VALUES (...) clause
   *
   * @example
   * // Simple INSERT with objects
   * const newUsers = [
   *   { name: 'Alice', email: 'alice@example.com', age: 25 },
   *   { name: 'Bob', email: 'bob@example.com', age: 30 }
   * ];
   * const query = sql`
   *   INSERT INTO users ${sql.insertValues(newUsers)}
   *   RETURNING id
   * `;
   * // Generates: INSERT INTO users (name, email, age) VALUES ($1, $2, $3), ($4, $5, $6) RETURNING id
   *
   * @example
   * // INSERT with specific columns only
   * const userData = [
   *   { name: 'Alice', email: 'alice@example.com', internal_id: 'ignore_this' },
   *   { name: 'Bob', email: 'bob@example.com', internal_id: 'ignore_this' }
   * ];
   * const query = sql`
   *   INSERT INTO users ${sql.insertValues(userData, ['name', 'email'])}
   * `;
   * // Only inserts name and email, ignores internal_id
   */
  insertValues(
    v: Array<Record<string, SQLPrimitive>>,
    ...cols: Array<string>
  ): SQLFragment<never>;

  /**
   * Generate a parenthesized value list expression for use in SQL IN clauses.
   * Creates a comma-separated list of values wrapped in parentheses.
   *
   * @param v - Array of primitive values to include in the list
   * @returns SQLFragment containing the value list expression
   *
   * @example
   * // Simple IN clause with numbers
   * const userIds = [1, 2, 3, 4];
   * const query = sql`
   *   SELECT * FROM users
   *   WHERE id IN ${sql.list(userIds)}
   * `;
   * // Generates: WHERE id IN ($1, $2, $3, $4)
   *
   * @example
   * // IN clause with strings
   * const statuses = ['active', 'pending', 'verified'];
   * const query = sql`
   *   SELECT * FROM accounts
   *   WHERE status IN ${sql.list(statuses)}
   * `;
   * // Generates: WHERE status IN ($1, $2, $3)
   */
  list(v: Array<SQLPrimitive>): SQLFragment<never>;

  /**
   * Generate a parenthesized value list expression by extracting a specific key
   * from an array of objects. Useful for IN clauses when you have objects but
   * only need one field for the comparison.
   *
   * @template K - The key type (must be a string)
   * @param v - Array of objects containing the key to extract
   * @param key - The key to extract from each object
   * @returns SQLFragment containing the value list expression
   *
   * @example
   * // Extract IDs from user objects
   * const users = [
   *   { id: 1, name: 'Alice', status: 'active' },
   *   { id: 2, name: 'Bob', status: 'pending' },
   *   { id: 3, name: 'Charlie', status: 'active' }
   * ];
   * const query = sql`
   *   SELECT * FROM photos
   *   WHERE user_id IN ${sql.list(users, 'id')}
   * `;
   * // Generates: WHERE user_id IN ($1, $2, $3) with values [1, 2, 3]
   *
   * @example
   * // Extract email addresses for lookup
   * const contacts = [
   *   { email: 'alice@example.com', type: 'friend' },
   *   { email: 'bob@example.com', type: 'colleague' }
   * ];
   * const query = sql`
   *   SELECT * FROM users
   *   WHERE email IN ${sql.list(contacts, 'email')}
   * `;
   * // Generates: WHERE email IN ($1, $2) with values ['alice@example.com', 'bob@example.com']
   */
  list<K extends string>(
    v: Array<Record<K, SQLPrimitive>>,
    key: K,
  ): SQLFragment<never>;

  /**
   * Join multiple SQL fragments with a specified separator.
   * Useful for building dynamic WHERE clauses, column lists, or other SQL constructs.
   *
   * @param sep - The separator to use between fragments ('AND', 'OR', or ',')
   * @param frags - Array of SQL fragments to join together
   * @returns SQLFragment containing the joined fragments
   *
   * @example
   * // Building dynamic WHERE clauses
   * const conditions: Array<SQLFragment> = [];
   * if (name) conditions.push(sql`name ILIKE ${`%${name}%`}`);
   * if (minAge) conditions.push(sql`age >= ${minAge}`);
   * if (status) conditions.push(sql`status = ${status}`);
   *
   * const query = sql`
   *   SELECT * FROM users
   *   ${conditions.length > 0 ? sql`WHERE ${sql.join('AND', conditions)}` : sql``}
   * `;
   *
   * @example
   * // Building column lists
   * const columns = ['id', 'name', 'email'].map(col => sql.id(col));
   * const query = sql`SELECT ${sql.join(',', columns)} FROM users`;
   * // Generates: SELECT "id", "name", "email" FROM users
   *
   * @example
   * // Building OR conditions
   * const searchTerms = ['alice', 'bob', 'charlie'];
   * const nameConditions = searchTerms.map(term => sql`name ILIKE ${`%${term}%`}`);
   * const query = sql`
   *   SELECT * FROM users
   *   WHERE ${sql.join('OR', nameConditions)}
   * `;
   * // Generates: WHERE name ILIKE $1 OR name ILIKE $2 OR name ILIKE $3
   */
  join(sep: "AND" | "OR" | ",", frags: Array<SQLFragment>): SQLFragment<never>;
}

/**
 * SQL interface for building and executing queries.
 * Provides template string functionality and helper methods for safe query construction.
 *
 * @example
 * // Basic usage in a database helper function
 * export async function getUserByID(sql: SQL, accountID: number, id: number) {
 *   const users = await sql<{id: number, name: string, email: string}>`
 *     SELECT id, name, email
 *     FROM users
 *     WHERE account_id = ${accountID} AND id = ${id}
 *   `;
 *
 *   return users[0] ?? null;
 * }
 *
 * @example
 * // Bulk insert with error handling
 * export async function createPhotos(sql: SQL, accountID: number, photoData: Array<{filename: string, path: string}>) {
 *   if (photoData.length === 0) {
 *     return [];
 *   }
 *
 *   const dataWithAccount = photoData.map(photo => ({
 *     ...photo,
 *     account_id: accountID,
 *     created_at: new Date()
 *   }));
 *
 *   const newPhotos = await sql<{id: number}>`
 *     INSERT INTO photos ${sql.insertValues(dataWithAccount)}
 *     RETURNING id
 *   `;
 *
 *   return newPhotos;
 * }
 */
export interface SQL extends AnySQL {
  /** Always false for non-transaction SQL operations */
  readonly inTransaction: false;

  /**
   * Execute a function within a database transaction.
   * Automatically handles BEGIN, COMMIT, and ROLLBACK operations.
   *
   * @template T - Return type of the transaction function
   * @param fn - Function to execute within the transaction, receives TransactionSQL instance
   * @returns Promise that resolves to the return value of the transaction function
   * @throws Re-throws any error from the transaction function after rolling back
   *
   */
  transaction<T>(fn: (tx: TransactionSQL) => Promise<T>): Promise<T>;
}

/**
 * SQL interface for building and executing queries within a database transaction.
 *
 * This interface is provided to transaction callback functions and ensures all
 * queries execute within the same transaction context. It provides the same
 * query building capabilities as the regular SQL interface but within a
 * transactional scope.
 *
 * All queries executed through this interface will be automatically committed
 * if the transaction function completes successfully, or rolled back if an
 * error occurs.
 *
 */
export interface TransactionSQL extends AnySQL {
  /** Always true for transaction SQL operations */
  readonly inTransaction: true;

  /**
   * Template string function for building SQL queries within a transaction.
   * Identical to the base AnySQL template function but executes within
   * the transaction context.
   *
   * @template T - The expected return type when the query is executed
   * @param strings - Template string literals
   * @param values - Values to be parameterized in the query
   * @returns SQLFragment that executes within the current transaction
   *
   * @example
   * await sql.transaction(async (tx) => {
   *   // This query runs in the transaction
   *   const result = await tx<{id: number}>`
   *     INSERT INTO photos (account_id, filename)
   *     VALUES (${accountID}, ${filename})
   *     RETURNING id
   *   `;
   *   return result[0];
   * });
   */
  <T = unknown>(
    strings: TemplateStringsArray,
    ...values: Array<SQLFragment | SQLPrimitive>
  ): SQLFragment<T>;
}

export class SQLClient {
  readonly _sql: Bun.SQL;
  readonly sql: SQL;
  constructor(options: Bun.SQL.Options | Bun.SQL) {
    // These are the two methods we use in practice, so sniff them explicitly.
    // This e.g., makes mocking in the tests a bit easier
    if ("unsafe" in options && "reserve" in options) {
      this._sql = options;
    } else {
      this._sql = new Bun.SQL({
        ...options,
        bigint: true,
      });
    }
    this.sql = this.makeSQL();
  }

  private makeSQL(): SQL {
    const sql = <T>(
      strings: TemplateStringsArray,
      ...values: Array<SQLFragment | Array<SQLFragment> | SQLPrimitive>
    ) => sqlTemplate<T>(this._sql, strings, values);
    sql.inTransaction = false as const;
    sql.id = (s: string) => id(this._sql, s);
    sql.unsafe = <T>(sql: string) => unsafe<T>(this._sql, sql);
    sql.values = (
      vs: Array<Array<SQLPrimitive> | Record<string, SQLPrimitive>>,
      ...cols: Array<string>
    ) => values(this._sql, vs, cols);
    sql.valuesT = (
      table: string,
      vs: Array<Record<string, SQLPrimitive>>,
      ...cols: Array<string>
    ) => valuesT(table, this._sql, vs, cols);
    sql.insertValues = (
      vs: Array<Record<string, SQLPrimitive>>,
      ...cols: Array<string>
    ) => insertValues(this._sql, vs, cols);
    sql.transaction = <T>(fn: (tx: TransactionSQL) => Promise<T>) =>
      transaction(this._sql, fn);
    sql.join = (sep: "AND" | "OR" | ",", frags: Array<SQLFragment>) =>
      join(this._sql, sep, frags);
    sql.list = (
      values: Array<SQLPrimitive> | Array<Record<string, SQLPrimitive>>,
      key?: string,
    ) => list(this._sql, values, key);

    return sql;
  }
}
