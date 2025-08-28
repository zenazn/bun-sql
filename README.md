# bun-sql
An opinionated alternate take on the [Bun SQL][bun] user-facing API.

This library just provides a query builder that targets the Postgres SQL
dialect. The underlying communication to the database is still handled by Bun.

In theory, Bun could be swapped out with a different SQL client, and Postgres
could be swapped with a different SQL dialect. In practice, that's maintenance
work I'm not excited to do. Bun and Postgres are nice, and are what I personally
use! You're welcome to fork the code here to accomplish other goals.

[bun]: https://bun.com/docs/api/sql

## Examples

```typescript
// Pass Bun.SQL or connection options to SQLClient
const sql = new SQLClient(Bun.sql).sql;

// SELECT name, email FROM users WHERE id = $1
// params: $1 = 123
// (future examples will interpolate parameters for clarity)
const users = await sql<{name: string; email: string}>`
  SELECT name, email FROM users WHERE id = ${123}
`;
console.log(users[0]); // { name: 'Alice', email: 'alice@example.com' }

// SELECT created_at FROM "user"
// (correctly handles Postgres reserved keywords)
await sql`SELECT ${sql.id('created_at')} FROM ${sql.id('user')}`;

const userIds = [1, 2, 3];
// SELECT id, name FROM users WHERE id IN (1, 2, 3)
await sql<{id: number, name: string}>`
  SELECT id, name FROM users WHERE id IN ${sql.list(userIds)}
`;

const photos = [
  { id: 2, filename: 'photo2.jpg' },
  { id: 3, filename: 'photo3.jpg' }
];
//  SELECT album_id FROM album_photos WHERE photo_id IN (2, 3)
await sql<{id: number, name: string}>`
  SELECT album_id FROM album_photos WHERE photo_id IN ${sql.list(photos, 'id')}
`;

const newUsers = [
  { name: 'Bob', email: 'bob@example.com', age: 28 },
  { name: 'Charlie', email: 'charlie@example.com', age: 35 }
];
// INSERT INTO users (name, email, age) VALUES ('Bob', 'bob@example.com', 28), ('Charlie', 'charlie@example.com', 35)
await sql<{id: number}>`
  INSERT INTO users ${sql.insertValues(newUsers)} RETURNING id
`;

// INSERT INTO users (name, age) VALUES ('Bob', 28), ('Charlie', 35)
await sql<{id: number}>`
  INSERT INTO users ${sql.insertValues(newUsers, 'name', 'age')} RETURNING id
`;

// Do bulk updates using a temporary table
const statusUpdates = [
  { id: 1, new_status: 'active' },
  { id: 2, new_status: 'suspended' }
];
// UPDATE users SET status = t.new_status
// FROM (VALUES (1, 'active'), (2, 'suspended')) AS t(id, new_status)
// WHERE users.id = t.id RETURNING users.id, users.status
const updateResult = await sql<{id: number, status: string}>`
  UPDATE users
  SET status = t.new_status
  FROM ${sql.valuesT('t', statusUpdates)}
  WHERE users.id = t.id
  RETURNING users.id, users.status
`;

// Embed queries into other queries
const cte = sql`SELECT id FROM users WHERE status = ${status}`;
await sql`
  WITH active_users AS (${cte})
  SELECT a.id, b.id
  FROM active_users a
  LEFT JOIN active_users b WHERE a.parent_id = b.id
`:

async function search(name?: string, minAge?: number, status?: string) => {
  // Dynamically construct WHERE clauses
  const conditions: Array<SQLFragment> = [];
  if (name) conditions.push(sql`name = ${name}`);
  if (minAge) conditions.push(sql`age >= ${minAge}`);
  if (searchStatus) conditions.push(sql`status = ${searchStatus}`);

  // Dynamically construct SELECT columns
  const select = [sql.id('id')]
  if (name === undefined) {
    select.push(sql.id('name'));
  }

  // e.g., SELECT id, name FROM users WHERE age >= 18 AND status = 'active';
  return await sql<{id: number, name: string}>`
    SELECT ${sql.join(',', select)} FROM users
    ${conditions.length > 0 ? sql`WHERE ${sql.join('AND', conditions)}` : sql``}
  `;
};

// Transactions
const result = await sql.transaction(async (tx) => {
  await tx`UPDATE accounts SET balance = balance - 100 WHERE id = ${fromAccountId}`;
  await tx`UPDATE accounts SET balance = balance + 100 WHERE id = ${toAccountId}`;
});

const locations = [
  {name: 'NYC', coordinates: [40.7128, -74.0060]},
  {name: 'SF', coordinates: [37.7749, -122.4194]},
]
// INSERT INTO locations (name, coordinates) VALUES ('NYC', ARRAY[40.7128, -74.0060]), ('SF', ARRAY[37.7749, -122.4194])
await sql`INSERT INTO locations ${sql.insertValues(locations)}`;
```

## Explicit instead of do-what-I-mean

The main way in which this library differs from Bun's SQL interface is that it
offers several explicit helpers in place of Bun's single do-what-I-mean helper.

While it's in theory possible to infer from context what you are trying to
accomplish when embedding values in a template string, it's a lot of work! The
full grammar of a SQL query is complex, which means there's a lot of cases to
consider, and as of late August 2025 the Bun SQL interface has a [few cases][dwim]
where it doesn't (yet!) do what I mean.

This library instead offers a few explicit helpers, like `.values(...)` for
assembling a `VALUES (...), ...` expression, or `.list([...])` for writing a
`WHERE IN (...)` parenthesized expression list. In return, this library promises
to behave predictably.

[dwim]: https://github.com/oven-sh/bun/issues/22164

## Easy to introspect

As of late August 2025, one thing I found difficult in Bun's SQL interface was
inspecting the queries it was assembling. In this library, I went out of my way
to make it easy to see what was going on. The core `SQLFragment` type exposes
`query`, the SQL query (with `$1` placeholders) that it would send, if asked,
and `params`, an array of placeholder values. This even looks nice when
`console.log`-ed.

If this library's helpers are producing unexpected or invalid SQL, please open
an issue!
