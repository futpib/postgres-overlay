
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

test.serial('select', async t => {
	const { lowerPool, upperPool } = t.context;

	const { rows: lowerRows } = await lowerPool.query('SELECT * FROM various_types;');
	const { rows: upperRows } = await upperPool.query('SELECT * FROM various_types;');

	t.deepEqual(upperRows, lowerRows);
});

test.serial('delete', async t => {
	const { lowerPool, upperPool } = t.context;

	await upperPool.query('DELETE FROM various_types WHERE id = 3;');

	const { rows: lowerRows } = await lowerPool.query('SELECT * FROM various_types WHERE id != 3;');
	const { rows: upperRows } = await upperPool.query('SELECT * FROM various_types;');

	t.deepEqual(upperRows, lowerRows);
});
