
import {
	any,
	equals,
	filter,
	prop,
	sortBy,
} from 'sanctuary';

import test from 'ava';

import pgEscape from 'pg-escape';

import { poolOptionsFromEnv, main } from '../..';

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
		sampleValues: [
			'DEFAULT',
			pgEscape.literal('foo'),
			'NOW()',
		],
	},

	{
		name: 'compound_primary_key',
		integerColumnName: 'foo_id',
		sampleValues: [
			4,
			4,
			pgEscape.literal('foo'),
		],
	},

	{
		name: 'no_primary_key',
		readOnly: true,
		sampleValues: [
			pgEscape.literal('foo'),
		],
	},
];

const selectMacro = async (t, table) => {
	const { lowerPool, upperPool } = t.context;

	const { rows: lowerRows } = await lowerPool.query(`SELECT * FROM ${table.name};`);
	const { rows: upperRows } = await upperPool.query(`SELECT * FROM ${table.name};`);

	t.deepEqual(upperRows, lowerRows);
};

tables.filter(table => !table.readOnly).forEach(table => {
	test.serial(`${table.name} select`, selectMacro, table);

	test.serial(`${table.name} delete`, async t => {
		const { lowerPool, upperPool } = t.context;

		await upperPool.query(
			`DELETE FROM ${table.name} WHERE ${table.integerColumnName} = 3;`
		);

		const { rows: lowerRows } = await lowerPool.query(
			`SELECT * FROM ${table.name} WHERE ${table.integerColumnName} != 3;`
		);
		const { rows: upperRows } = await upperPool.query(
			`SELECT * FROM ${table.name};`
		);

		t.deepEqual(upperRows, lowerRows);
	});

	test.serial(`${table.name} update`, async t => {
		const { upperPool } = t.context;

		await upperPool.query(
			`UPDATE ${table.name} SET text = 'foo' WHERE ${table.integerColumnName} = 3;`
		);

		const { rows: upperRows } = await upperPool.query(
			`SELECT * FROM ${table.name} WHERE ${table.integerColumnName} = 3 AND text = 'foo';`
		);

		t.deepEqual(upperRows.length, 1);
	});

	test.serial(`${table.name} insert`, async t => {
		const { lowerPool, upperPool } = t.context;

		const values = table.sampleValues.join(', ');

		await upperPool.query(
			`INSERT INTO ${table.name} VALUES (${values});`
		);

		const { rows: upperRows } = await upperPool.query(
			`SELECT * FROM ${table.name};`
		);

		const { rows: lowerRows } = await lowerPool.query(
			`SELECT * FROM ${table.name};`
		);

		const anyEquals = x => any(equals(x));
		const newUpperValues = filter(x => !anyEquals(x)(lowerRows))(upperRows);

		t.is(newUpperValues.length, 1);
	});

	test.serial(`${table.name} select again`, selectMacro, table);
});

tables.filter(table => table.readOnly).forEach(table => {
	test.serial(`${table.name} select`, selectMacro, table);

	test.serial(`${table.name} delete (read-only)`, async t => {
		const { upperPool } = t.context;

		await t.throwsAsync(() => upperPool.query(
			`DELETE FROM ${table.name} WHERE ${table.integerColumnName} = 3;`
		));
	});

	test.serial(`${table.name} update (read-only)`, async t => {
		const { upperPool } = t.context;

		await t.throwsAsync(() => upperPool.query(
			`UPDATE ${table.name} SET text = 'foo' WHERE true;`
		));
	});

	test.serial(`${table.name} insert (read-only)`, async t => {
		const { upperPool } = t.context;

		const values = table.sampleValues.join(', ');

		await t.throwsAsync(() => upperPool.query(
			`INSERT INTO ${table.name} VALUES (${values});`
		));
	});

	test.serial(`${table.name} select again`, selectMacro, table);
});
