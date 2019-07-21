
const {
	map,
	pipe,
	fromPairs,
	pairs,
	alt,
	get,
	maybeToNullable,
	prop,
	chain,
	I,

	Pair,

	Nothing,
	Just,
} = require('sanctuary');

const { Pool } = require('pg');
const pgEscape = require('pg-escape');

const poolOptionsFromEnv = ({ envPrefix, processEnv }) => pipe([
	pairs,
	map(([ key, default_ ]) => {
		const envKey = [ envPrefix, key ].join('_').toUpperCase();
		const value = get(Boolean)(envKey)(processEnv);
		return Pair(key)(maybeToNullable(alt(value)(default_)));
	}),
	fromPairs,
	({
		port,
		max,
		...options
	}) => ({
		port: parseInt(port, 10),
		max: parseInt(max, 10),
		...options,
	}),
])({
	user: Nothing,
	host: Nothing,
	database: Nothing,
	password: Nothing,
	port: Just('5432'),
	max: Just('10'),
});

const withPool = async (options, f) => {
	const pool = new Pool(options);
	try {
		return await f(pool);
	} finally {
		await pool.end();
	}
};

const unique = xs => [ ...new Set(xs) ];

const lowerSchemaNameToUpperSchemaName = schemaname => 'overlay_lower_' + schemaname;

const lowerSchemaNameToUpperDeletedSchemaName = schemaname => 'overlay_upper_deleted_' + schemaname;
const lowerSchemaNameToUpperInsertedSchemaName = schemaname => 'overlay_upper_inserted_' + schemaname;

const lowerSchemaNameToUpperDeleteRuleSchemaName = schemaname => 'overlay_upper_delete_rule_' + schemaname;

const TABLES_QUERY = `SELECT schemaname, tablename
FROM pg_catalog.pg_tables
WHERE tableowner = current_user AND NOT (schemaname = ANY($1::text[]));`;

const COLUMNS_QUERY = `SELECT column_name, data_type, is_nullable = 'YES' as is_nullable, column_default
FROM information_schema.columns 
WHERE table_schema = $1 AND table_name = $2;`;

const PRIMARY_KEYS_QUERY = `SELECT a.attname as column_name, format_type(a.atttypid, a.atttypmod) AS data_type
FROM pg_index i
JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
WHERE i.indrelid = $1::regclass AND i.indisprimary;`;

const CREATE_FDW_EXTENSION_QUERY = 'CREATE EXTENSION IF NOT EXISTS postgres_fdw;';

const CREATE_FDW_SERVER_QUERY = `CREATE SERVER IF NOT EXISTS lower
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (host $1, port $2, dbname $3, updatable 'false');`;

const CREATE_USER_MAPPING_QUERY = `CREATE USER MAPPING IF NOT EXISTS FOR $1
SERVER lower
OPTIONS (user $2, password $3);`;

const CREATE_SCHEMA_QUERY = 'CREATE SCHEMA IF NOT EXISTS $1;';

const EXISTING_TABLES_QUERY = `SELECT DISTINCT table_name
FROM information_schema.tables
WHERE table_schema = $1;`;

const IMPORT_FOREIGN_SCHEMA_QUERY = `IMPORT FOREIGN SCHEMA $1
EXCEPT ($2)
FROM SERVER lower INTO $3;`;

const CREATE_TABLE = 'CREATE TABLE IF NOT EXISTS $1.$2 ($3);';

const CREATE_VIEW = `CREATE OR REPLACE VIEW $1.$2
AS SELECT $3.$4.*
FROM $3.$4
	LEFT JOIN $5.$6
		ON $7
WHERE $8;`;

const CREATE_DELETE_RULE = `CREATE OR REPLACE RULE $1__$2 AS ON DELETE TO $3.$4
DO INSTEAD
INSERT INTO $5.$6 VALUES ($7);`;

const CREATE_RESET_FUNCTION = `CREATE OR REPLACE FUNCTION overlay_reset()
RETURNS void
AS $$
BEGIN
$1
END
$$ LANGUAGE plpgsql;`;

const DELETE_FROM_TABLE_QUERY = 'DELETE FROM $1.$2;';

const setupOverlay = ({ lowerOptions, upperOptions }) => withPool(lowerOptions, lowerPool => withPool(upperOptions, async upperPool => {
	let { rows: lowerTables } = await lowerPool.query(
		TABLES_QUERY,
		[
			[ 'pg_catalog', 'information_schema' ],
		],
	);

	lowerTables = await Promise.all(lowerTables.map(async table => {
		const { rows: columns } = await lowerPool.query(
			COLUMNS_QUERY,
			[
				table.schemaname,
				table.tablename,
			],
		);

		return {
			...table,
			columns,
		};
	}));

	lowerTables = await Promise.all(lowerTables.map(async table => {
		const { rows: primaryKeys } = await lowerPool.query(
			PRIMARY_KEYS_QUERY,
			[
				[ table.schemaname, table.tablename ].join('.'),
			],
		);

		return {
			...table,
			primaryKeys,
		};
	}));

	console.log(JSON.stringify({ lowerTables }, null, 2));

	await upperPool.query(CREATE_FDW_EXTENSION_QUERY);

	await upperPool.query(
		CREATE_FDW_SERVER_QUERY
			.replace('$1', pgEscape.literal(lowerOptions.host))
			.replace('$2', pgEscape.literal(String(lowerOptions.port)))
			.replace('$3', pgEscape.literal(lowerOptions.database))
	);

	await upperPool.query(
		CREATE_USER_MAPPING_QUERY
			.replace('$1', pgEscape.string(upperOptions.user))
			.replace('$2', pgEscape.literal(lowerOptions.user))
			.replace('$3', pgEscape.literal(lowerOptions.password))
	);

	const lowerSchemas = pipe([
		map(prop('schemaname')),
		unique,
	])(lowerTables);

	const createAllSchemas = schemaNameMapper => Promise.all(lowerSchemas.map(async schemaname => {
		const upperSchemaName = schemaNameMapper(schemaname);

		await upperPool.query(
			CREATE_SCHEMA_QUERY
				.replace('$1', pgEscape.string(upperSchemaName))
		);
	}));

	await createAllSchemas(lowerSchemaNameToUpperSchemaName);

	await Promise.all(lowerSchemas.map(async schemaname => {
		const upperSchemaName = lowerSchemaNameToUpperSchemaName(schemaname);

		const { rows: existingTables } = await upperPool.query(
			EXISTING_TABLES_QUERY,
			[
				upperSchemaName,
			],
		);

		await upperPool.query(
			IMPORT_FOREIGN_SCHEMA_QUERY
				.replace('$1', pgEscape.string(schemaname))
				.replace(
					'$2',
					existingTables
						.concat([ {
							table_name: '_postgres_overlay_hack_unique_table_name_that_should_never_exist',
						} ])
						.map(row => pgEscape.string(row.table_name))
						.join(', ')
				)
				.replace('$3', pgEscape.string(upperSchemaName))
		);
	}));

	await createAllSchemas(lowerSchemaNameToUpperDeletedSchemaName);

	await Promise.all(lowerTables.map(async table => {
		const upperSchemaName = lowerSchemaNameToUpperDeletedSchemaName(table.schemaname);

		await upperPool.query(
			CREATE_TABLE
				.replace('$1', pgEscape.string(upperSchemaName))
				.replace('$2', pgEscape.string(table.tablename))
				.replace(
					'$3',
					table.primaryKeys
						.map(primaryKey => [
							primaryKey.column_name,
							primaryKey.data_type,
							'NOT NULL',
						].join(' '))
				)
		);
	}));

	await createAllSchemas(lowerSchemaNameToUpperInsertedSchemaName);

	await Promise.all(lowerTables.map(async table => {
		const upperSchemaName = lowerSchemaNameToUpperInsertedSchemaName(table.schemaname);

		await upperPool.query(
			CREATE_TABLE
				.replace('$1', pgEscape.string(upperSchemaName))
				.replace('$2', pgEscape.string(table.tablename))
				.replace(
					'$3',
					table.columns
						.map(column => [
							column.column_name,
							column.data_type,
							!column.is_nullable && 'NOT NULL',
						].filter(Boolean).join(' '))
				)
		);
	}));

	await createAllSchemas(I);

	await Promise.all(lowerTables.map(async table => {
		const upperSchemaName = lowerSchemaNameToUpperSchemaName(table.schemaname);
		const upperDeletedSchemaName = lowerSchemaNameToUpperDeletedSchemaName(table.schemaname);
		const upperDeleteRuleSchemaName = lowerSchemaNameToUpperDeleteRuleSchemaName(table.schemaname);

		await upperPool.query(
			CREATE_VIEW
				.replace('$1', pgEscape.string(table.schemaname))
				.replace('$2', pgEscape.string(table.tablename))
				.replace(/\$3/g, pgEscape.string(upperSchemaName))
				.replace(/\$4/g, pgEscape.string(table.tablename))
				.replace('$5', pgEscape.string(upperDeletedSchemaName))
				.replace('$6', pgEscape.string(table.tablename))
				.replace(
					'$7',
					table.primaryKeys
						.map(primaryKey => (
							[
								[
									pgEscape.string(upperSchemaName),
									pgEscape.string(table.tablename),
									pgEscape.string(primaryKey.column_name),
								].join('.'),
								'=',
								[
									pgEscape.string(upperDeletedSchemaName),
									pgEscape.string(table.tablename),
									pgEscape.string(primaryKey.column_name),
								].join('.'),
							].join(' ')
						))
						.join(' AND ')
				)
				.replace(
					'$8',
					[
						[
							pgEscape.string(upperDeletedSchemaName),
							pgEscape.string(table.tablename),
							pgEscape.string(table.primaryKeys[0].column_name),
						].join('.'),
						'IS NULL',
					].join(' '),
				)
		);

		await upperPool.query(
			CREATE_DELETE_RULE
				.replace('$1', pgEscape.string(upperDeleteRuleSchemaName))
				.replace('$2', pgEscape.string(table.tablename))
				.replace('$3', pgEscape.string(table.schemaname))
				.replace('$4', pgEscape.string(table.tablename))
				.replace('$5', pgEscape.string(upperDeletedSchemaName))
				.replace('$6', pgEscape.string(table.tablename))
				.replace(
					'$7',
					table.primaryKeys
						.map(primaryKey => 'OLD.' + pgEscape.string(primaryKey.column_name))
						.join(', ')
				)
		);
	}));

	await upperPool.query(
		CREATE_RESET_FUNCTION
			.replace(
				'$1',
				chain(table => (
					[
						lowerSchemaNameToUpperDeletedSchemaName,
						lowerSchemaNameToUpperInsertedSchemaName,
					].map(schemaNameMapper => (
						DELETE_FROM_TABLE_QUERY
							.replace('$1', schemaNameMapper(table.schemaname))
							.replace('$2', pgEscape.string(table.tablename))
					))
				))(lowerTables).join('\n'),
			),
	);
}));

const main = () => setupOverlay({
	lowerOptions: poolOptionsFromEnv({ envPrefix: 'lower', processEnv: process.env }),
	upperOptions: poolOptionsFromEnv({ envPrefix: 'upper', processEnv: process.env }),
});

module.exports = {
	main,
	setupOverlay,
	poolOptionsFromEnv,
};
