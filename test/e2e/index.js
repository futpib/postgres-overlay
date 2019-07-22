
import test from 'ava';

import { poolOptionsFromEnv, main } from '../..';

const { Pool } = require('pg');

test.before(async t => {
	const lowerPool = new Pool(poolOptionsFromEnv({ envPrefix: 'lower', processEnv: process.env }));
	const upperPool = new Pool(poolOptionsFromEnv({ envPrefix: 'upper', processEnv: process.env }));

	Object.assign(t.context, { lowerPool, upperPool });

	await main();
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
	},
	{
		name: 'compound_primary_key',
		integerColumnName: 'foo_id',
	},
];

tables.forEach(table => {
	test.serial(`${table.name} select`, async t => {
		const { lowerPool, upperPool } = t.context;

		const { rows: lowerRows } = await lowerPool.query(`SELECT * FROM ${table.name};`);
		const { rows: upperRows } = await upperPool.query(`SELECT * FROM ${table.name};`);

		t.deepEqual(upperRows, lowerRows);
	});

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
});
