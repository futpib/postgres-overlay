
import {
	any,
	equals,
	filter,
	prop,
	sortBy,
} from 'sanctuary';

import test from 'ava';

import pgEscape from 'pg-escape';

import {
	main,
	poolOptionsFromEnv,
} from '../..';

const sql = (strings, ...identifiers) => {
	identifiers = identifiers.map(s => pgEscape.ident(s));
	return strings
		.map((string, index) => identifiers[index] ? (string + identifiers[index]) : string)
		.join('');
};

const { Pool } = require('pg');

test.before(async t => {
	const lowerPool = new Pool(poolOptionsFromEnv({ envPrefix: 'lower', processEnv: process.env }));
	const upperPool = new Pool(poolOptionsFromEnv({ envPrefix: 'upper', processEnv: process.env }));

	Object.assign(t.context, { lowerPool, upperPool });

	const result = await main();

	t.deepEqual(sortBy(prop('tableName'))(result.tables), [
		{
			readOnly: false,
			schemaName: 'public',
			tableName: 'compound_primary_key',
		},
		{
			readOnly: true,
			schemaName: 'public',
			tableName: 'no_primary_key',
		},
		{
			readOnly: false,
			schemaName: 'public',
			tableName: 'various_types',
		},
		{
			readOnly: false,
			schemaName: 'public',
			tableName: 'weird_name_слон_$1',
		},
	]);
});

test.after.always(async t => {
	const { lowerPool, upperPool } = t.context;
	await Promise.all([
		lowerPool && lowerPool.end(),
		upperPool && upperPool.end(),
	]);
});

test.beforeEach(async t => {
	const { upperPool } = t.context;
	await upperPool.query('SELECT overlay_reset();');
});

const tables = [
	{
		name: 'various_types',
		integerColumnName: 'id',
		insertValues: [
			1024,
			pgEscape.literal('foo'),
			'NOW()',
			pgEscape.literal('{{1,2,3},{4,5,6},{7,8,9}}'),
		],
	},

	{
		name: 'compound_primary_key',
		integerColumnName: 'foo_id',
		insertValues: [
			1024,
			4,
			pgEscape.literal('foo'),
		],
	},

	{
		name: 'no_primary_key',
		readOnly: true,
		integerColumnName: 'integer',
		insertValues: [
			pgEscape.literal('foo'),
		],
	},

	{
		name: 'weird_name_слон_$1',
		integerColumnName: 'id1_слон_$1',
		insertValues: [
			4,
			4,
		],
	},
];

const selectMacro = async (t, table) => {
	const { lowerPool, upperPool } = t.context;

	const { rows: lowerRows } = await lowerPool.query(sql`SELECT * FROM ${table.name};`);
	const { rows: upperRows } = await upperPool.query(sql`SELECT * FROM ${table.name};`);

	t.deepEqual(upperRows, lowerRows);
};

tables.filter(table => !table.readOnly).forEach(table => {
	test.serial(`${table.name} select`, selectMacro, table);

	test.serial(`${table.name} delete`, async t => {
		const { lowerPool, upperPool } = t.context;

		await upperPool.query(
			sql`DELETE FROM ${table.name} WHERE ${table.integerColumnName} = 3;`
		);

		const { rows: lowerRows } = await lowerPool.query(
			sql`SELECT * FROM ${table.name} WHERE ${table.integerColumnName} != 3;`
		);
		const { rows: upperRows } = await upperPool.query(
			sql`SELECT * FROM ${table.name};`
		);

		t.deepEqual(upperRows, lowerRows);
	});

	test.serial(`${table.name} update`, async t => {
		const { upperPool } = t.context;

		await upperPool.query(
			sql`UPDATE ${table.name} SET text = 'foo' WHERE ${table.integerColumnName} = 3;`
		);

		const { rows: upperRows } = await upperPool.query(
			sql`SELECT * FROM ${table.name} WHERE ${table.integerColumnName} = 3 AND text = 'foo';`
		);

		t.deepEqual(upperRows.length, 1);
	});

	test.serial(`${table.name} insert`, async t => {
		const { lowerPool, upperPool } = t.context;

		const values = table.insertValues.join(', ');

		await upperPool.query(
			(sql`INSERT INTO ${table.name} VALUES`) + `(${values});`
		);

		const { rows: upperRows } = await upperPool.query(
			sql`SELECT * FROM ${table.name};`
		);

		const { rows: lowerRows } = await lowerPool.query(
			sql`SELECT * FROM ${table.name};`
		);

		const anyEquals = x => any(equals(x));
		const newUpperValues = filter(x => !anyEquals(x)(lowerRows))(upperRows);

		t.is(newUpperValues.length, 1);
	});

	test.serial(`${table.name} insert & delete`, async t => {
		const { upperPool } = t.context;

		const { rows: rowsBefore } = await upperPool.query(
			sql`SELECT * FROM ${table.name};`
		);

		const values = table.insertValues.join(', ');

		const { rows: [ row ] } = await upperPool.query(
			(sql`INSERT INTO ${table.name} VALUES`) + ` (${values}) RETURNING *;`
		);

		await upperPool.query(
			(sql`DELETE FROM ${table.name} WHERE ${table.integerColumnName}`) + ` = ${row[table.integerColumnName]};`
		);

		const { rows: rowsAfter } = await upperPool.query(
			sql`SELECT * FROM ${table.name};`
		);

		t.deepEqual(
			sortBy(JSON.stringify)(rowsAfter),
			sortBy(JSON.stringify)(rowsBefore),
		);
	});

	test.serial(`${table.name} select again`, selectMacro, table);
});

tables.filter(table => table.readOnly).forEach(table => {
	test.serial(`${table.name} select`, selectMacro, table);

	const errorMessageRegex = /does not allow/;

	test.serial(`${table.name} delete (read-only)`, async t => {
		const { upperPool } = t.context;

		await t.throwsAsync(() => upperPool.query(
			sql`DELETE FROM ${table.name} WHERE ${table.integerColumnName} = 3;`
		), errorMessageRegex);
	});

	test.serial(`${table.name} update (read-only)`, async t => {
		const { upperPool } = t.context;

		await t.throwsAsync(() => upperPool.query(
			sql`UPDATE ${table.name} SET text = 'foo' WHERE true;`
		), errorMessageRegex);
	});

	test.serial(`${table.name} insert (read-only)`, async t => {
		const { upperPool } = t.context;

		const values = table.insertValues.join(', ');

		await t.throwsAsync(() => upperPool.query(
			(sql`INSERT INTO ${table.name} VALUES`) + ` (${values});`
		), errorMessageRegex);
	});

	test.serial(`${table.name} select again`, selectMacro, table);
});
