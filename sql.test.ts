import { beforeEach, describe, it, expect } from 'bun:test';
import { SQLClient, SQLFragment, type SQL } from './sql';

// Mock Bun.SQL for testing
class MockBunSQL {
  private queries: Array<{ query: string; params: Array<unknown> }> = [];

  unsafe<T>(
    query: string,
    params: Array<unknown> = []
  ): { then: (fn: (result: T) => unknown) => Promise<unknown> } {
    this.queries.push({ query, params });
    return {
      then: (fn: (result: T) => unknown) => Promise.resolve(fn([] as T)),
    };
  }

  async reserve(): Promise<MockBunSQL & { [Symbol.dispose]: () => void }> {
    const conn = new MockBunSQL();
    conn.queries = this.queries; // Share query log
    let disposed = false;
    return Object.assign(conn, {
      [Symbol.dispose]: () => {
        disposed = true;
      },
      get isDisposed() {
        return disposed;
      },
    });
  }

  getQueries() {
    return [...this.queries];
  }

  clearQueries() {
    this.queries = [];
  }
}

describe('SQLClient', () => {
  let mockSQL: MockBunSQL;
  let client: SQLClient;
  let sql: SQL;

  beforeEach(() => {
    mockSQL = new MockBunSQL();
    client = new SQLClient(mockSQL as unknown as Bun.SQL);
    sql = client.sql;
    mockSQL.clearQueries();
  });

  describe('template string functionality', () => {
    it('should handle basic template strings', async () => {
      const userId = 123;
      const name = 'Alice';

      await sql<{
        id: number;
      }>`SELECT * FROM users WHERE id = ${userId} AND name = ${name}`;

      expect(mockSQL.getQueries()).toEqual([
        {
          query: 'SELECT * FROM users WHERE id = $1 AND name = $2',
          params: [123, 'Alice'],
        },
      ]);
    });

    it('should handle null values', async () => {
      const nullValue = null;

      await sql`UPDATE users SET deleted_at = ${nullValue} WHERE id = ${1}`;

      expect(mockSQL.getQueries()).toEqual([
        {
          query: 'UPDATE users SET deleted_at = $1 WHERE id = $2',
          params: [null, 1],
        },
      ]);
    });

    it('should handle Date values', async () => {
      const date = new Date('2023-01-01T00:00:00Z');

      await sql`INSERT INTO events (created_at) VALUES (${date})`;

      expect(mockSQL.getQueries()).toEqual([
        {
          query: 'INSERT INTO events (created_at) VALUES ($1)',
          params: [date],
        },
      ]);
    });

    it('should handle boolean values', async () => {
      const isActive = true;
      const isDeleted = false;

      await sql`UPDATE users SET active = ${isActive}, deleted = ${isDeleted}`;

      expect(mockSQL.getQueries()).toEqual([
        {
          query: 'UPDATE users SET active = $1, deleted = $2',
          params: [true, false],
        },
      ]);
    });

    it('should handle bigint values', async () => {
      const bigId = BigInt('9223372036854775807');

      await sql`SELECT * FROM users WHERE big_id = ${bigId}`;

      expect(mockSQL.getQueries()).toEqual([
        {
          query: 'SELECT * FROM users WHERE big_id = $1',
          params: [bigId],
        },
      ]);
    });

    it('should handle Buffer values', async () => {
      const buffer = Buffer.from('test data');

      await sql`INSERT INTO files (data) VALUES (${buffer})`;

      expect(mockSQL.getQueries()).toEqual([
        {
          query: 'INSERT INTO files (data) VALUES ($1)',
          params: [buffer],
        },
      ]);
    });

    it('should handle nested SQLFragments', async () => {
      const whereClause = sql`WHERE age > ${25}`;

      console.log(sql`SELECT * FROM users ${whereClause}`);
      await sql`SELECT * FROM users ${whereClause}`;

      expect(mockSQL.getQueries()).toEqual([
        {
          query: 'SELECT * FROM users WHERE age > $1',
          params: [25],
        },
      ]);
    });

    it('should handle arrays of SQLFragments', async () => {
      const conditions = [
        sql`age > ${25}`,
        sql` AND `,
        sql`status = ${'active'}`,
        sql` AND `,
        sql`created_at > ${new Date('2023-01-01')}`,
      ];

      await sql`SELECT * FROM users WHERE ${conditions}`;

      expect(mockSQL.getQueries()).toEqual([
        {
          query:
            'SELECT * FROM users WHERE age > $1 AND status = $2 AND created_at > $3',
          params: [25, 'active', new Date('2023-01-01')],
        },
      ]);
    });

    it('should handle arrays of SQLFragments with join() method', async () => {
      const conditions = [
        sql`age > ${25}`,
        sql`status = ${'active'}`,
        sql`created_at > ${new Date('2023-01-01')}`,
      ];

      await sql`SELECT * FROM users WHERE ${sql.join('AND', conditions)}`;

      expect(mockSQL.getQueries()).toEqual([
        {
          query:
            'SELECT * FROM users WHERE age > $1 AND status = $2 AND created_at > $3',
          params: [25, 'active', new Date('2023-01-01')],
        },
      ]);
    });
  });

  describe('id() method', () => {
    it('should handle simple identifiers', async () => {
      const tableName = 'users';

      await sql`SELECT * FROM ${sql.id(tableName)}`;

      expect(mockSQL.getQueries()).toEqual([
        {
          query: 'SELECT * FROM users',
          params: [],
        },
      ]);
    });

    it('should quote reserved keywords', async () => {
      const columnName = 'order';

      await sql`SELECT ${sql.id(columnName)} FROM products`;

      expect(mockSQL.getQueries()).toEqual([
        {
          query: 'SELECT "order" FROM products',
          params: [],
        },
      ]);
    });

    it('should quote identifiers with spaces', async () => {
      const tableName = 'user data';

      await sql`SELECT * FROM ${sql.id(tableName)}`;

      expect(mockSQL.getQueries()).toEqual([
        {
          query: 'SELECT * FROM "user data"',
          params: [],
        },
      ]);
    });

    it('should quote identifiers starting with numbers', async () => {
      const columnName = '2023_data';

      await sql`SELECT ${sql.id(columnName)} FROM table1`;

      expect(mockSQL.getQueries()).toEqual([
        {
          query: 'SELECT "2023_data" FROM table1',
          params: [],
        },
      ]);
    });

    it('should handle identifiers with special characters', async () => {
      const tableName = 'table-name';

      await sql`SELECT * FROM ${sql.id(tableName)}`;

      expect(mockSQL.getQueries()).toEqual([
        {
          query: 'SELECT * FROM "table-name"',
          params: [],
        },
      ]);
    });

    it('should escape quotes in identifiers', async () => {
      const columnName = 'col"umn';

      await sql`SELECT ${sql.id(columnName)} FROM table1`;

      expect(mockSQL.getQueries()).toEqual([
        {
          query: 'SELECT "col""umn" FROM table1',
          params: [],
        },
      ]);
    });

    it('should handle unicode characters with U& syntax', async () => {
      const tableName = 'table_π';

      await sql`SELECT * FROM ${sql.id(tableName)}`;

      expect(mockSQL.getQueries()).toEqual([
        {
          query: 'SELECT * FROM U&"table_\\03c0"',
          params: [],
        },
      ]);
    });
  });

  describe('unsafe() method', () => {
    it('should inject raw SQL', async () => {
      const orderBy = 'ORDER BY created_at DESC';

      await sql`SELECT * FROM users ${sql.unsafe(orderBy)}`;

      expect(mockSQL.getQueries()).toEqual([
        {
          query: 'SELECT * FROM users ORDER BY created_at DESC',
          params: [],
        },
      ]);
    });

    it('should work with typed unsafe queries', async () => {
      const dynamicWhere = "WHERE status = 'active'";

      await sql`SELECT * FROM users ${sql.unsafe(dynamicWhere)}`;

      expect(mockSQL.getQueries()).toEqual([
        {
          query: "SELECT * FROM users WHERE status = 'active'",
          params: [],
        },
      ]);
    });
  });

  describe('array handling', () => {
    it('should convert arrays to PostgreSQL ARRAY syntax', async () => {
      const tags = ['photo', 'family', 'vacation'];

      await sql`INSERT INTO posts (tags) VALUES (${tags})`;

      expect(mockSQL.getQueries()).toEqual([
        {
          query: 'INSERT INTO posts (tags) VALUES (ARRAY[$1, $2, $3])',
          params: ['photo', 'family', 'vacation'],
        },
      ]);
    });

    it('should handle nested arrays', async () => {
      const matrix = [
        [1, 2],
        [3, 4],
      ];

      await sql`INSERT INTO matrices (data) VALUES (${matrix})`;

      expect(mockSQL.getQueries()).toEqual([
        {
          query:
            'INSERT INTO matrices (data) VALUES (ARRAY[[$1, $2], [$3, $4]])',
          params: [1, 2, 3, 4],
        },
      ]);
    });

    it('should handle empty arrays', async () => {
      const emptyArray: Array<string> = [];

      await sql`UPDATE users SET tags = ${emptyArray}`;

      expect(mockSQL.getQueries()).toEqual([
        {
          query: 'UPDATE users SET tags = ARRAY[]',
          params: [],
        },
      ]);
    });
  });

  describe('values() method', () => {
    it('should handle array of arrays', async () => {
      const data = [
        [1, 'Alice'],
        [2, 'Bob'],
      ];

      await sql`INSERT INTO users (id, name) ${sql.values(data)}`;

      expect(mockSQL.getQueries()).toEqual([
        {
          query: 'INSERT INTO users (id, name) VALUES ($1, $2), ($3, $4)',
          params: [1, 'Alice', 2, 'Bob'],
        },
      ]);
    });

    it('should handle array of objects', async () => {
      const data = [
        { id: 1, name: 'Alice', email: 'alice@example.com' },
        { id: 2, name: 'Bob', email: 'bob@example.com' },
      ];

      await sql`INSERT INTO users (id, name, email) ${sql.values(data, 'id', 'name', 'email')}`;

      expect(mockSQL.getQueries()).toEqual([
        {
          query:
            'INSERT INTO users (id, name, email) VALUES ($1, $2, $3), ($4, $5, $6)',
          params: [
            1,
            'Alice',
            'alice@example.com',
            2,
            'Bob',
            'bob@example.com',
          ],
        },
      ]);
    });

    it('should handle array of objects with specific columns', async () => {
      const data = [
        {
          id: 1,
          name: 'Alice',
          email: 'alice@example.com',
          internal: 'ignore',
        },
        { id: 2, name: 'Bob', email: 'bob@example.com', internal: 'ignore' },
      ];

      await sql`INSERT INTO users (id, name) ${sql.values(data, 'id', 'name')}`;

      expect(mockSQL.getQueries()).toEqual([
        {
          query: 'INSERT INTO users (id, name) VALUES ($1, $2), ($3, $4)',
          params: [1, 'Alice', 2, 'Bob'],
        },
      ]);
    });

    it('should throw error for empty values array', () => {
      expect(() => sql.values([])).toThrow(
        'VALUES must have at least one element'
      );
    });

    it('should throw error for object missing required column', () => {
      const data = [{ id: 1, name: 'Alice' }, { id: 2 }]; // Missing 'name' in second object
      // @ts-expect-error Typescript catches the error, but we're testing runtime behavior here
      expect(() => sql.values(data, 'id', 'name')).toThrow(
        'Object {"id":2} missing column name'
      );
    });

    it('should throw error for empty columns', () => {
      const data = [{}];
      // @ts-expect-error Typescript catches the error, but we're testing runtime behavior here
      expect(() => sql.values(data)).toThrow(
        'VALUES must have at least one column'
      );
    });
  });

  describe('valuesT() method', () => {
    it('should create VALUES table with alias', async () => {
      const data = [
        { id: 1, status: 'active' },
        { id: 2, status: 'inactive' },
      ];

      await sql`UPDATE users SET status = t.status FROM ${sql.valuesT('t', data)} WHERE users.id = t.id`;

      expect(mockSQL.getQueries()).toEqual([
        {
          query:
            'UPDATE users SET status = t.status FROM VALUES ($1, $2), ($3, $4) AS t(id, status) WHERE users.id = t.id',
          params: [1, 'active', 2, 'inactive'],
        },
      ]);
    });

    it('should handle custom column specification', async () => {
      const data = [
        { user_id: 1, new_status: 'active', other: 'ignore' },
        { user_id: 2, new_status: 'inactive', other: 'ignore' },
      ];

      await sql`SELECT * FROM ${sql.valuesT('t', data, 'user_id', 'new_status')}`;

      expect(mockSQL.getQueries()).toEqual([
        {
          query:
            'SELECT * FROM VALUES ($1, $2), ($3, $4) AS t(user_id, new_status)',
          params: [1, 'active', 2, 'inactive'],
        },
      ]);
    });

    it('should throw error for empty values array', () => {
      expect(() => sql.valuesT('t', [])).toThrow(
        'VALUES must have at least one element'
      );
    });

    it('should throw error for empty columns', () => {
      const data = [{}];
      expect(() => sql.valuesT('t', data)).toThrow(
        'VALUES must have at least one column'
      );
    });
  });

  describe('insertValues() method', () => {
    it('should create column list and values for INSERT', async () => {
      const data = [
        { name: 'Alice', email: 'alice@example.com', age: 25 },
        { name: 'Bob', email: 'bob@example.com', age: 30 },
      ];

      await sql`INSERT INTO users ${sql.insertValues(data)} RETURNING id`;

      expect(mockSQL.getQueries()).toEqual([
        {
          query:
            'INSERT INTO users (name, email, age) VALUES ($1, $2, $3), ($4, $5, $6) RETURNING id',
          params: [
            'Alice',
            'alice@example.com',
            25,
            'Bob',
            'bob@example.com',
            30,
          ],
        },
      ]);
    });

    it('should handle specific columns', async () => {
      const data = [
        { name: 'Alice', email: 'alice@example.com', internal: 'ignore' },
        { name: 'Bob', email: 'bob@example.com', internal: 'ignore' },
      ];

      await sql`INSERT INTO users ${sql.insertValues(data, 'name', 'email')}`;

      expect(mockSQL.getQueries()).toEqual([
        {
          query: 'INSERT INTO users (name, email) VALUES ($1, $2), ($3, $4)',
          params: ['Alice', 'alice@example.com', 'Bob', 'bob@example.com'],
        },
      ]);
    });

    it('should throw error for empty values array', () => {
      expect(() => sql.insertValues([])).toThrow(
        'VALUES must have at least one element'
      );
    });

    it('should throw error for empty columns', () => {
      const data = [{}];
      expect(() => sql.insertValues(data)).toThrow(
        'VALUES must have at least one column'
      );
    });
  });

  describe('list() method', () => {
    it('should create IN clause with primitive values', async () => {
      const ids = [1, 2, 3];

      await sql`SELECT * FROM users WHERE id IN ${sql.list(ids)}`;

      expect(mockSQL.getQueries()).toEqual([
        {
          query: 'SELECT * FROM users WHERE id IN ($1, $2, $3)',
          params: [1, 2, 3],
        },
      ]);
    });

    it('should handle string values in IN clause', async () => {
      const statuses = ['active', 'pending', 'verified'];

      await sql`SELECT * FROM accounts WHERE status IN ${sql.list(statuses)}`;

      expect(mockSQL.getQueries()).toEqual([
        {
          query: 'SELECT * FROM accounts WHERE status IN ($1, $2, $3)',
          params: ['active', 'pending', 'verified'],
        },
      ]);
    });

    it('should extract values from objects using key', async () => {
      const users = [
        { id: 1, name: 'Alice', status: 'active' },
        { id: 2, name: 'Bob', status: 'pending' },
        { id: 3, name: 'Charlie', status: 'active' },
      ];

      await sql`SELECT * FROM photos WHERE user_id IN ${sql.list(users, 'id')}`;

      expect(mockSQL.getQueries()).toEqual([
        {
          query: 'SELECT * FROM photos WHERE user_id IN ($1, $2, $3)',
          params: [1, 2, 3],
        },
      ]);
    });

    it('should extract string values from objects using key', async () => {
      const contacts = [
        { email: 'alice@example.com', type: 'friend' },
        { email: 'bob@example.com', type: 'colleague' },
      ];

      await sql`SELECT * FROM users WHERE email IN ${sql.list(contacts, 'email')}`;

      expect(mockSQL.getQueries()).toEqual([
        {
          query: 'SELECT * FROM users WHERE email IN ($1, $2)',
          params: ['alice@example.com', 'bob@example.com'],
        },
      ]);
    });

    it('should handle single value', async () => {
      const ids = [42];

      await sql`SELECT * FROM users WHERE id IN ${sql.list(ids)}`;

      expect(mockSQL.getQueries()).toEqual([
        {
          query: 'SELECT * FROM users WHERE id IN ($1)',
          params: [42],
        },
      ]);
    });

    it('should work in complex queries', async () => {
      const userIds = [1, 2, 3];
      const statuses = ['active', 'verified'];

      await sql`
        SELECT p.*, u.name
        FROM photos p
        LEFT JOIN users u ON p.user_id = u.id
        WHERE u.id IN ${sql.list(userIds)}
        AND u.status IN ${sql.list(statuses)}
        ORDER BY p.created_at DESC
      `;

      expect(mockSQL.getQueries()).toEqual([
        {
          query: `
        SELECT p.*, u.name
        FROM photos p
        LEFT JOIN users u ON p.user_id = u.id
        WHERE u.id IN ($1, $2, $3)
        AND u.status IN ($4, $5)
        ORDER BY p.created_at DESC
      `,
          params: [1, 2, 3, 'active', 'verified'],
        },
      ]);
    });

    it('should throw error for empty array', () => {
      expect(() => sql.list([])).toThrow(
        'Value lists must have at least one element'
      );
    });

    it('should throw error for empty array with key extraction', () => {
      expect(() => sql.list([], 'id')).toThrow(
        'Value lists must have at least one element'
      );
    });
  });

  describe('join() method', () => {
    it('should join fragments with AND separator', async () => {
      const conditions = [
        sql`age > ${25}`,
        sql`status = ${'active'}`,
        sql`created_at > ${new Date('2023-01-01')}`,
      ];

      await sql`SELECT * FROM users WHERE ${sql.join('AND', conditions)}`;

      expect(mockSQL.getQueries()).toEqual([
        {
          query:
            'SELECT * FROM users WHERE age > $1 AND status = $2 AND created_at > $3',
          params: [25, 'active', new Date('2023-01-01')],
        },
      ]);
    });

    it('should join fragments with OR separator', async () => {
      const conditions = [
        sql`name = ${'Alice'}`,
        sql`name = ${'Bob'}`,
        sql`name = ${'Charlie'}`,
      ];

      await sql`SELECT * FROM users WHERE ${sql.join('OR', conditions)}`;

      expect(mockSQL.getQueries()).toEqual([
        {
          query:
            'SELECT * FROM users WHERE name = $1 OR name = $2 OR name = $3',
          params: ['Alice', 'Bob', 'Charlie'],
        },
      ]);
    });

    it('should join fragments with comma separator', async () => {
      const columns = [
        sql.id('id'),
        sql.id('name'),
        sql.id('email'),
        sql.id('created_at'),
      ];

      await sql`SELECT ${sql.join(',', columns)} FROM users`;

      expect(mockSQL.getQueries()).toEqual([
        {
          query: 'SELECT id, name, email, created_at FROM users',
          params: [],
        },
      ]);
    });

    it('should handle empty array of fragments', async () => {
      const conditions: Array<SQLFragment> = [];

      await sql`SELECT * FROM users${sql.join('AND', conditions)}`;

      expect(mockSQL.getQueries()).toEqual([
        {
          query: 'SELECT * FROM users',
          params: [],
        },
      ]);
    });

    it('should handle single fragment', async () => {
      const conditions = [sql`status = ${'active'}`];

      await sql`SELECT * FROM users WHERE ${sql.join('AND', conditions)}`;

      expect(mockSQL.getQueries()).toEqual([
        {
          query: 'SELECT * FROM users WHERE status = $1',
          params: ['active'],
        },
      ]);
    });

    it('should work with complex dynamic WHERE clauses', async () => {
      const filters = {
        name: 'Alice',
        minAge: 25,
        status: 'active',
      };

      const conditions = [];
      if (filters.name) conditions.push(sql`name ILIKE ${`%${filters.name}%`}`);
      if (filters.minAge) conditions.push(sql`age >= ${filters.minAge}`);
      if (filters.status) conditions.push(sql`status = ${filters.status}`);

      const whereClause =
        conditions.length > 0
          ? sql`WHERE ${sql.join('AND', conditions)}`
          : sql``;

      await sql`SELECT * FROM users ${whereClause}`;

      expect(mockSQL.getQueries()).toEqual([
        {
          query:
            'SELECT * FROM users WHERE name ILIKE $1 AND age >= $2 AND status = $3',
          params: ['%Alice%', 25, 'active'],
        },
      ]);
    });

    it('should work with mixed fragment types', async () => {
      const searchTerms = ['alice', 'bob'];
      const nameConditions = searchTerms.map(
        (term) => sql`name ILIKE ${`%${term}%`}`
      );
      const statusConditions = [
        sql`status = ${'active'}`,
        sql`status = ${'pending'}`,
      ];

      const allConditions = [
        sql`(${sql.join('OR', nameConditions)})`,
        sql`(${sql.join('OR', statusConditions)})`,
      ];

      await sql`SELECT * FROM users WHERE ${sql.join('AND', allConditions)}`;

      expect(mockSQL.getQueries()).toEqual([
        {
          query:
            'SELECT * FROM users WHERE (name ILIKE $1 OR name ILIKE $2) AND (status = $3 OR status = $4)',
          params: ['%alice%', '%bob%', 'active', 'pending'],
        },
      ]);
    });

    it('should work in transaction context', async () => {
      const conditions = [sql`age > ${30}`, sql`status = ${'active'}`];

      await sql.transaction(async (tx) => {
        await tx`UPDATE users SET updated_at = NOW() WHERE ${tx.join('AND', conditions)}`;
      });

      const queries = mockSQL.getQueries();
      expect(queries).toEqual([
        { query: 'BEGIN', params: [] },
        {
          query:
            'UPDATE users SET updated_at = NOW() WHERE age > $1 AND status = $2',
          params: [30, 'active'],
        },
        { query: 'COMMIT', params: [] },
      ]);
    });

    it('should handle nested joins with complex structures', async () => {
      const userFilters = [sql`u.active = ${true}`, sql`u.role = ${'admin'}`];
      const photoFilters = [
        sql`p.status = ${'published'}`,
        sql`p.created_at > ${new Date('2023-01-01')}`,
      ];

      const query = sql`
        SELECT u.name, COUNT(p.id) as photo_count
        FROM users u
        LEFT JOIN photos p ON u.id = p.user_id
        WHERE ${sql.join('AND', userFilters)}
        AND ${sql.join('AND', photoFilters)}
        GROUP BY u.id, u.name
        ORDER BY photo_count DESC
      `;

      await query;

      expect(mockSQL.getQueries()).toEqual([
        {
          query:
            '\n        SELECT u.name, COUNT(p.id) as photo_count\n        FROM users u\n        LEFT JOIN photos p ON u.id = p.user_id\n        WHERE u.active = $1 AND u.role = $2\n        AND p.status = $3 AND p.created_at > $4\n        GROUP BY u.id, u.name\n        ORDER BY photo_count DESC\n      ',
          params: [true, 'admin', 'published', new Date('2023-01-01')],
        },
      ]);
    });

    it('should work with insertValues and join for bulk operations', async () => {
      const userData = [
        { name: 'Alice', email: 'alice@example.com', age: 25 },
        { name: 'Bob', email: 'bob@example.com', age: 30 },
      ];

      const updateConditions = [
        sql`created_at = NOW()`,
        sql`updated_at = NOW()`,
        sql`status = ${'active'}`,
      ];

      await sql`
        WITH inserted AS (
          INSERT INTO users ${sql.insertValues(userData)}
          RETURNING id
        )
        UPDATE user_profiles
        SET ${sql.join(',', updateConditions)}
        WHERE user_id IN (SELECT id FROM inserted)
      `;

      expect(mockSQL.getQueries()).toEqual([
        {
          query:
            '\n        WITH inserted AS (\n          INSERT INTO users (name, email, age) VALUES ($1, $2, $3), ($4, $5, $6)\n          RETURNING id\n        )\n        UPDATE user_profiles\n        SET created_at = NOW(), updated_at = NOW(), status = $7\n        WHERE user_id IN (SELECT id FROM inserted)\n      ',
          params: [
            'Alice',
            'alice@example.com',
            25,
            'Bob',
            'bob@example.com',
            30,
            'active',
          ],
        },
      ]);
    });

    it('should handle join with valuesT for complex updates', async () => {
      const updates = [
        { id: 1, status: 'active', priority: 10 },
        { id: 2, status: 'inactive', priority: 5 },
      ];

      const setClause = [
        sql`status = t.status`,
        sql`priority = t.priority`,
        sql`updated_at = NOW()`,
      ];

      await sql`
        UPDATE users
        SET ${sql.join(',', setClause)}
        FROM ${sql.valuesT('t', updates)}
        WHERE users.id = t.id
      `;

      expect(mockSQL.getQueries()).toEqual([
        {
          query:
            '\n        UPDATE users\n        SET status = t.status, priority = t.priority, updated_at = NOW()\n        FROM VALUES ($1, $2, $3), ($4, $5, $6) AS t(id, status, priority)\n        WHERE users.id = t.id\n      ',
          params: [1, 'active', 10, 2, 'inactive', 5],
        },
      ]);
    });

    it.each([
      {
        name: 'should handle join with empty conditions gracefully',
        conditionCount: 0,
        expectedQuery: 'SELECT * FROM users ',
        expectedParams: [],
      },
      {
        name: 'should handle join with single condition',
        conditionCount: 1,
        expectedQuery: 'SELECT * FROM users WHERE id = $1',
        expectedParams: [1],
      },
      {
        name: 'should handle join with multiple conditions',
        conditionCount: 3,
        expectedQuery:
          'SELECT * FROM users WHERE id > $1 AND status = $2 AND created_at > $3',
        expectedParams: [10, 'active', new Date('2023-01-01')],
      },
    ])('$name', async ({ conditionCount, expectedQuery, expectedParams }) => {
      const fragments = [];
      if (conditionCount === 1) {
        fragments.push(sql`id = ${1}`);
      } else if (conditionCount === 3) {
        fragments.push(sql`id > ${10}`);
        fragments.push(sql`status = ${'active'}`);
        fragments.push(sql`created_at > ${new Date('2023-01-01')}`);
      }

      const whereClause =
        fragments.length > 0 ? sql`WHERE ${sql.join('AND', fragments)}` : sql``;

      await sql`SELECT * FROM users ${whereClause}`;

      expect(mockSQL.getQueries()).toEqual([
        {
          query: expectedQuery,
          params: expectedParams,
        },
      ]);
    });
  });

  describe('transactions', () => {
    it('should execute functions within transactions', async () => {
      const result = await sql.transaction(async (tx) => {
        await tx`INSERT INTO users (name) VALUES (${'Alice'})`;
        await tx`INSERT INTO posts (title) VALUES (${'Hello World'})`;
        return 'success';
      });

      expect(result).toBe('success');
      expect(mockSQL.getQueries()).toEqual([
        { query: 'BEGIN', params: [] },
        { query: 'INSERT INTO users (name) VALUES ($1)', params: ['Alice'] },
        {
          query: 'INSERT INTO posts (title) VALUES ($1)',
          params: ['Hello World'],
        },
        { query: 'COMMIT', params: [] },
      ]);
    });

    it('should rollback on error', async () => {
      try {
        await sql.transaction(async (tx) => {
          await tx`INSERT INTO users (name) VALUES (${'Alice'})`;
          throw new Error('Test error');
        });
      } catch (error) {
        expect((error as Error).message).toBe('Test error');
      }

      expect(mockSQL.getQueries()).toEqual([
        { query: 'BEGIN', params: [] },
        { query: 'INSERT INTO users (name) VALUES ($1)', params: ['Alice'] },
        { query: 'ROLLBACK', params: [] },
      ]);
    });

    it('should provide transaction SQL interface', async () => {
      await sql.transaction(async (tx) => {
        expect(tx.inTransaction).toBe(true);
        expect(typeof tx.id).toBe('function');
        expect(typeof tx.unsafe).toBe('function');
        expect(typeof tx.values).toBe('function');
        expect(typeof tx.valuesT).toBe('function');
        expect(typeof tx.insertValues).toBe('function');
        expect(typeof tx.list).toBe('function');

        // Test that transaction methods work
        await tx`SELECT ${tx.id('column')} FROM ${tx.id('table')}`;
      });

      expect(mockSQL.getQueries()).toEqual([
        { query: 'BEGIN', params: [] },
        { query: 'SELECT "column" FROM "table"', params: [] },
        { query: 'COMMIT', params: [] },
      ]);
    });

    it('should handle transaction SQL helper methods', async () => {
      await sql.transaction(async (tx) => {
        const data = [{ id: 1, name: 'Alice' }];
        const ids = [1, 2, 3];

        await tx`INSERT INTO users ${tx.insertValues(data)}`;
        await tx`SELECT * FROM ${tx.valuesT('t', data)}`;
        await tx`SELECT * FROM users WHERE id IN ${tx.list(ids)}`;
        await tx`SELECT * FROM users WHERE id IN ${tx.list(data, 'id')}`;
        await tx`${tx.unsafe('SELECT 1')}`;

        return 'success';
      });

      expect(mockSQL.getQueries()).toEqual([
        { query: 'BEGIN', params: [] },
        {
          query: 'INSERT INTO users (id, name) VALUES ($1, $2)',
          params: [1, 'Alice'],
        },
        {
          query: 'SELECT * FROM VALUES ($1, $2) AS t(id, name)',
          params: [1, 'Alice'],
        },
        {
          query: 'SELECT * FROM users WHERE id IN ($1, $2, $3)',
          params: [1, 2, 3],
        },
        {
          query: 'SELECT * FROM users WHERE id IN ($1)',
          params: [1],
        },
        { query: 'SELECT 1', params: [] },
        { query: 'COMMIT', params: [] },
      ]);
    });
  });

  describe('edge cases and error handling', () => {
    it('should handle complex nested queries', async () => {
      const subquery = sql`SELECT id FROM users WHERE active = ${true}`;
      const conditions = [
        sql`created_at > ${new Date('2023-01-01')}`,
        sql`status IN (${['active', 'pending']})`,
      ];

      await sql`
        SELECT p.*
        FROM photos p
        WHERE p.user_id IN (${subquery})
        AND ${sql.join('AND', conditions)}
        ORDER BY ${sql.unsafe('p.created_at DESC')}
        LIMIT ${10}
      `;

      expect(mockSQL.getQueries()).toEqual([
        {
          query:
            '\n        SELECT p.*\n        FROM photos p\n        WHERE p.user_id IN (SELECT id FROM users WHERE active = $1)\n        AND created_at > $2 AND status IN (ARRAY[$3, $4])\n        ORDER BY p.created_at DESC\n        LIMIT $5\n      ',
          params: [true, new Date('2023-01-01'), 'active', 'pending', 10],
        },
      ]);
    });

    it('should handle empty template strings', async () => {
      await sql`SELECT 1`;

      expect(mockSQL.getQueries()).toEqual([
        {
          query: 'SELECT 1',
          params: [],
        },
      ]);
    });

    it('should handle single parameter template', async () => {
      await sql`SELECT ${42}`;

      expect(mockSQL.getQueries()).toEqual([
        {
          query: 'SELECT $1',
          params: [42],
        },
      ]);
    });
  });
});
