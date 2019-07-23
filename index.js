
const invariant = require('invariant');

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
	concat,
	I,

	Pair,

	Nothing,
	Just,
} = require('sanctuary');

const { Pool } = require('pg');
const pgEscape = require('pg-escape');

const escapeIdentifier = string => {
	invariant(
		string && typeof string === 'string',
		'Expected non-empty string (identifier to escape), instead got: %s',
		string,
	);
	return pgEscape.ident(string);
};

const substitute = (query, values) => {
	try {
		return query.replace(/#\{(.+?)\}/g, (_, key) => {
			const value = values[key];

			if (Array.isArray(value)) {
				return value.map(escapeIdentifier).join('.');
			}

			if (value.unsafe) {
				return value.unsafe;
			}

			return escapeIdentifier(value);
		});
	} catch (error) {
		console.warn(
			'An error occured while substituting query:',
			query,
			'with values:',
			values,
		);
		throw error;
	}
};

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

const lowerSchemaNameToUpperSchemaName = concat('overlay_lower_');

const lowerSchemaNameToUpperDeletedSchemaName = concat('overlay_upper_deleted_');
const lowerSchemaNameToUpperInsertedSchemaName = concat('overlay_upper_inserted_');

const lowerSchemaNameToUpperDeleteRuleSchemaName = concat('overlay_upper_delete_rule_');
const lowerSchemaNameToUpperInsertRuleSchemaName = concat('overlay_upper_insert_rule_');
const lowerSchemaNameToUpperUpdateRuleSchemaName = concat('overlay_upper_update_rule_');

const lowerSchemaNameToUpperDefaultFunctionSchemaName = concat('overlay_upper_default_function_');

const TABLES_QUERY = `SELECT schemaname, tablename
FROM pg_catalog.pg_tables
WHERE tableowner = current_user AND NOT (schemaname = ANY($1::text[]));`;

const COLUMNS_QUERY = `SELECT column_name, udt_name::regtype as data_type, is_nullable = 'YES' as is_nullable, column_default
FROM information_schema.columns
WHERE table_schema = $1 AND table_name = $2;`;

const PRIMARY_KEYS_QUERY = `SELECT a.attname as column_name, format_type(a.atttypid, a.atttypmod) AS data_type
FROM pg_index i
JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
WHERE i.indrelid = $1::regclass AND i.indisprimary;`;

const CREATE_FDW_EXTENSION_QUERY = 'CREATE EXTENSION IF NOT EXISTS postgres_fdw;';

const CREATE_FDW_SERVER_QUERY = `CREATE SERVER IF NOT EXISTS lower
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (host #{host}, port #{port}, dbname #{dbname}, updatable 'false');`;

const CREATE_USER_MAPPING_QUERY = `CREATE USER MAPPING IF NOT EXISTS FOR #{user_ident}
SERVER lower
OPTIONS (user #{user}, password #{password});`;

const CREATE_SCHEMA_QUERY = 'CREATE SCHEMA IF NOT EXISTS #{name};';

const EXISTING_TABLES_QUERY = `SELECT DISTINCT table_name
FROM information_schema.tables
WHERE table_schema = $1;`;

const IMPORT_FOREIGN_SCHEMA_QUERY = `IMPORT FOREIGN SCHEMA #{foreign_name}
EXCEPT (#{except})
FROM SERVER lower INTO #{local_name};`;

const CREATE_TABLE = 'CREATE TABLE IF NOT EXISTS #{table_name} (#{columns});';

const CREATE_READ_ONLY_VIEW = `CREATE OR REPLACE VIEW #{view_name}
AS SELECT *
FROM #{foreign_table_name};`;

const CREATE_VIEW = `CREATE OR REPLACE VIEW #{view_name} AS
SELECT #{foreign_table_name}.*
FROM (
	SELECT #{primary_key}
	FROM #{inserted_table_name}
UNION
	SELECT #{primary_key}
	FROM #{foreign_table_name}
EXCEPT
	SELECT #{primary_key}
	FROM #{deleted_table_name}
) _ids
	LEFT JOIN #{foreign_table_name}
		ON #{foreign_join_condition};`;

const CREATE_DEFAULT_FUNCTION = `CREATE OR REPLACE FUNCTION #{function_name}()
RETURNS #{return_type}
AS $$
BEGIN
RETURN 1 + (
	SELECT MAX(#{column_name})
	FROM #{table_name}
);
END
$$ LANGUAGE plpgsql;`;

const ALTER_VIEW_DEFAULT = `ALTER VIEW #{view_name}
ALTER COLUMN #{column_name}
SET DEFAULT #{function_name}();`;

const CREATE_DELETE_RULE = `CREATE OR REPLACE RULE #{rule_name} AS ON DELETE TO #{table_name}
DO INSTEAD (
	INSERT INTO #{deleted_table_name} VALUES (#{deleted_primary_key});
	DELETE FROM #{inserted_table_name} WHERE (#{inserted_where_condition});
);`;

const CREATE_UPDATE_RULE = `CREATE OR REPLACE RULE #{rule_name} AS ON UPDATE TO #{table_name}
DO INSTEAD
INSERT INTO #{inserted_table_name} VALUES (#{inserted_values})
ON CONFLICT (#{inserted_primary_key}) DO UPDATE SET #{update_expression};`;

const CREATE_INSERT_RULE = `CREATE OR REPLACE RULE #{rule_name} AS ON INSERT TO #{table_name}
DO INSTEAD (
	DELETE FROM #{deleted_table_name} WHERE #{deleted_where_condition};
	INSERT INTO #{inserted_table_name} VALUES (#{inserted_values}) RETURNING *;
);`;

const CREATE_RESET_FUNCTION = `CREATE OR REPLACE FUNCTION overlay_reset()
RETURNS void
AS $$
BEGIN
#{function_body}
END
$$ LANGUAGE plpgsql;`;

const DELETE_FROM_TABLE_QUERY = 'DELETE FROM #{table_name};';

const setupOverlay = ({ lowerOptions, upperOptions }) => withPool(lowerOptions, lowerPool => withPool(upperOptions, async upperPool => {
	const result = {
		tables: [],
	};

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

		const readOnly = primaryKeys.length === 0;

		result.tables.push({
			schemaName: table.schemaname,
			tableName: table.tablename,
			readOnly,
		});

		return {
			...table,
			primaryKeys,
			readOnly,
		};
	}));

	await upperPool.query(CREATE_FDW_EXTENSION_QUERY);

	await upperPool.query(
		substitute(
			CREATE_FDW_SERVER_QUERY,
			{
				host: { unsafe: pgEscape.literal(lowerOptions.host) },
				port: { unsafe: pgEscape.literal(String(lowerOptions.port)) },
				dbname: { unsafe: pgEscape.literal(lowerOptions.database) },
			},
		)
	);

	await upperPool.query(
		substitute(
			CREATE_USER_MAPPING_QUERY,
			{
				user_ident: upperOptions.user,
				user: { unsafe: pgEscape.literal(lowerOptions.user) },
				password: { unsafe: pgEscape.literal(lowerOptions.password) },
			},
		)
	);

	const lowerSchemas = pipe([
		map(prop('schemaname')),
		unique,
	])(lowerTables);

	const createAllSchemas = schemaNameMapper => Promise.all(lowerSchemas.map(async schemaname => {
		const upperSchemaName = schemaNameMapper(schemaname);

		await upperPool.query(
			substitute(
				CREATE_SCHEMA_QUERY,
				{
					name: upperSchemaName,
				},
			)
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
			substitute(
				IMPORT_FOREIGN_SCHEMA_QUERY,
				{
					foreign_name: schemaname,
					local_name: upperSchemaName,
					except: {
						unsafe: existingTables
							.concat([ {
								table_name: '_postgres_overlay_hack_unique_table_name_that_should_never_exist',
							} ])
							.map(row => escapeIdentifier(row.table_name))
							.join(', '),
					},
				},
			)
		);
	}));

	await createAllSchemas(lowerSchemaNameToUpperDeletedSchemaName);

	await Promise.all(lowerTables.map(async table => {
		if (table.readOnly) {
			return;
		}

		const upperDeletedSchemaName = lowerSchemaNameToUpperDeletedSchemaName(table.schemaname);

		await upperPool.query(
			substitute(
				CREATE_TABLE,
				{
					table_name: [ upperDeletedSchemaName, table.tablename ],
					columns: { unsafe: [
						...table.primaryKeys
							.map(primaryKey => (
								[
									escapeIdentifier(primaryKey.column_name),
									primaryKey.data_type,
									'NOT NULL',
								].join(' ')
							)),

						[
							'PRIMARY KEY (',
							table.primaryKeys
								.map(primaryKey => escapeIdentifier(primaryKey.column_name))
								.join(', '),
							')',
						].join(' '),
					].join(', ') },
				},
			)
		);
	}));

	await createAllSchemas(lowerSchemaNameToUpperInsertedSchemaName);

	await Promise.all(lowerTables.map(async table => {
		if (table.readOnly) {
			return;
		}

		const upperInsertedSchemaName = lowerSchemaNameToUpperInsertedSchemaName(table.schemaname);

		await upperPool.query(
			substitute(
				CREATE_TABLE,
				{
					table_name: [ upperInsertedSchemaName, table.tablename ],
					columns: { unsafe: [
						...table.columns
							.map(column => [
								escapeIdentifier(column.column_name),
								column.data_type,
								!column.is_nullable && 'NOT NULL',
							].filter(Boolean).join(' ')),
						[
							'PRIMARY KEY (',
							table.primaryKeys
								.map(primaryKey => escapeIdentifier(primaryKey.column_name))
								.join(', '),
							')',
						].join(' '),
					].join(', ') },
				}
			)
		);
	}));

	await createAllSchemas(I);
	await createAllSchemas(lowerSchemaNameToUpperDefaultFunctionSchemaName);

	await Promise.all(lowerTables.map(async table => {
		const upperSchemaName = lowerSchemaNameToUpperSchemaName(table.schemaname);

		const upperDeletedSchemaName = lowerSchemaNameToUpperDeletedSchemaName(table.schemaname);
		const upperDeleteRuleSchemaName = lowerSchemaNameToUpperDeleteRuleSchemaName(table.schemaname);

		const upperInsertedSchemaName = lowerSchemaNameToUpperInsertedSchemaName(table.schemaname);

		const upperUpdateRuleSchemaName = lowerSchemaNameToUpperUpdateRuleSchemaName(table.schemaname);
		const upperInsertRuleSchemaName = lowerSchemaNameToUpperInsertRuleSchemaName(table.schemaname);

		if (table.readOnly) {
			await upperPool.query(
				substitute(
					CREATE_READ_ONLY_VIEW,
					{
						view_name: [ table.schemaname, table.tablename ],
						foreign_table_name: [ upperSchemaName, table.tablename ],
					},
				)
			);

			return;
		}

		await upperPool.query(
			substitute(
				CREATE_VIEW,
				{
					view_name: [ table.schemaname, table.tablename ],
					foreign_table_name: [ upperSchemaName, table.tablename ],
					deleted_table_name: [ upperDeletedSchemaName, table.tablename ],
					inserted_table_name: [ upperInsertedSchemaName, table.tablename ],

					primary_key: { unsafe: (
						table.primaryKeys
							.map(primaryKey => escapeIdentifier(primaryKey.column_name))
							.join(', ')
					) },

					foreign_join_condition: { unsafe: (
						table.primaryKeys
							.map(primaryKey => (
								[
									[
										escapeIdentifier(upperSchemaName),
										escapeIdentifier(table.tablename),
										escapeIdentifier(primaryKey.column_name),
									].join('.'),
									'=',
									[
										'_ids',
										escapeIdentifier(primaryKey.column_name),
									].join('.'),
								].join(' ')
							))
							.join(' AND ')
					) },

					select_expressions: { unsafe: (
						table.columns
							.map(column => (
								[
									'CASE WHEN',
									[
										escapeIdentifier(upperInsertedSchemaName),
										escapeIdentifier(table.tablename),
										escapeIdentifier(table.primaryKeys[0].column_name),
									].join('.'),
									'IS NOT NULL THEN',
									[
										escapeIdentifier(upperInsertedSchemaName),
										escapeIdentifier(table.tablename),
										escapeIdentifier(column.column_name),
									].join('.'),
									'ELSE',
									[
										escapeIdentifier(upperSchemaName),
										escapeIdentifier(table.tablename),
										escapeIdentifier(column.column_name),
									].join('.'),
									'END',
								].join(' ')
							))
							.join(', ')
					) },

					deleted_join_condition: { unsafe: (
						table.primaryKeys
							.map(primaryKey => (
								[
									[
										escapeIdentifier(upperSchemaName),
										escapeIdentifier(table.tablename),
										escapeIdentifier(primaryKey.column_name),
									].join('.'),
									'=',
									[
										escapeIdentifier(upperDeletedSchemaName),
										escapeIdentifier(table.tablename),
										escapeIdentifier(primaryKey.column_name),
									].join('.'),
								].join(' ')
							))
							.join(' AND ')
					) },

					inserted_join_condition: { unsafe: (
						table.primaryKeys
							.map(primaryKey => (
								[
									[
										escapeIdentifier(upperSchemaName),
										escapeIdentifier(table.tablename),
										escapeIdentifier(primaryKey.column_name),
									].join('.'),
									'=',
									[
										escapeIdentifier(upperInsertedSchemaName),
										escapeIdentifier(table.tablename),
										escapeIdentifier(primaryKey.column_name),
									].join('.'),
								].join(' ')
							))
							.join(' AND ')
					) },

					where_condition: { unsafe: [
						[
							escapeIdentifier(upperDeletedSchemaName),
							escapeIdentifier(table.tablename),
							escapeIdentifier(table.primaryKeys[0].column_name),
						].join('.'),
						'IS NULL',
					].join(' ') },
				}
			)
		);

		await Promise.all(
			table.columns
				.filter(column => Boolean(column.column_default))
				.map(async column => {
					const upperDefaultFunctionSchemaName = lowerSchemaNameToUpperDefaultFunctionSchemaName(table.schemaname);

					const functionName = [
						upperDefaultFunctionSchemaName,
						escapeIdentifier([
							table.tablename,
							column.column_name,
						].join('__')),
					];

					await upperPool.query(
						substitute(
							CREATE_DEFAULT_FUNCTION,
							{
								function_name: functionName,
								return_type: column.data_type,
								column_name: column.column_name,
								table_name: [ table.schemaname, table.tablename ],
							},
						)
					);

					await upperPool.query(
						substitute(
							ALTER_VIEW_DEFAULT,
							{
								function_name: functionName,
								view_name: [ table.schemaname, table.tablename ],
								column_name: column.column_name,
							},
						)
					);
				})
		);

		await upperPool.query(
			substitute(
				CREATE_DELETE_RULE,
				{
					rule_name: [ upperDeleteRuleSchemaName, table.tablename ].join('__'),
					table_name: [ table.schemaname, table.tablename ],
					deleted_table_name: [ upperDeletedSchemaName, table.tablename ],
					inserted_table_name: [ upperInsertedSchemaName, table.tablename ],

					deleted_primary_key: { unsafe: (
						table.primaryKeys
							.map(primaryKey => 'OLD.' + escapeIdentifier(primaryKey.column_name))
							.join(', ')
					) },

					inserted_where_condition: { unsafe: (
						table.primaryKeys
							.map(primaryKey => (
								[
									[
										escapeIdentifier(upperInsertedSchemaName),
										escapeIdentifier(table.tablename),
										escapeIdentifier(primaryKey.column_name),
									].join('.'),
									'=',
									'OLD.' + escapeIdentifier(primaryKey.column_name),
								].join(' ')
							))
							.join(' AND ')
					) },
				},
			)
		);

		await upperPool.query(
			substitute(
				CREATE_UPDATE_RULE,
				{
					rule_name: [ upperUpdateRuleSchemaName, table.tablename ].join('__'),
					table_name: [ table.schemaname, table.tablename ],
					inserted_table_name: [ upperInsertedSchemaName, table.tablename ],

					inserted_values: { unsafe: (
						table.columns
							.map(column => 'NEW.' + escapeIdentifier(column.column_name))
							.join(', ')
					) },

					inserted_primary_key: { unsafe: (
						table.primaryKeys
							.map(primaryKey => escapeIdentifier(primaryKey.column_name))
							.join(', ')
					) },

					update_expression: { unsafe: (
						table.columns
							.map(column => (
								[
									escapeIdentifier(column.column_name),
									'=',
									'EXCLUDED.' + escapeIdentifier(column.column_name),
								].join(' ')
							))
							.join(', ')
					) },
				},
			)
		);

		await upperPool.query(
			substitute(
				CREATE_INSERT_RULE,
				{
					rule_name: [ upperInsertRuleSchemaName, table.tablename ].join('__'),
					table_name: [ table.schemaname, table.tablename ],
					deleted_table_name: [ upperDeletedSchemaName, table.tablename ],
					inserted_table_name: [ upperInsertedSchemaName, table.tablename ],

					deleted_where_condition: { unsafe: (
						table.primaryKeys
							.map(primaryKey => (
								[
									[
										escapeIdentifier(upperDeletedSchemaName),
										escapeIdentifier(table.tablename),
										escapeIdentifier(primaryKey.column_name),
									].join('.'),
									'=',
									'NEW.' + escapeIdentifier(primaryKey.column_name),
								].join(' ')
							))
							.join(' AND ')
					) },

					inserted_values: { unsafe: (
						table.columns
							.map(column => 'NEW.' + escapeIdentifier(column.column_name))
							.join(', ')
					) },
				},
			)
		);
	}));

	await upperPool.query(
		substitute(
			CREATE_RESET_FUNCTION,
			{
				function_body: { unsafe: (
					chain(table => table.readOnly ? [] : (
						[
							lowerSchemaNameToUpperDeletedSchemaName,
							lowerSchemaNameToUpperInsertedSchemaName,
						].map(schemaNameMapper => substitute(
							DELETE_FROM_TABLE_QUERY,
							{
								table_name: [ schemaNameMapper(table.schemaname), table.tablename ],
							},
						))
					))(lowerTables).join('\n')
				) },
			}
		),
	);

	return result;
}));

const main = () => setupOverlay({
	lowerOptions: poolOptionsFromEnv({ envPrefix: 'lower', processEnv: process.env }),
	upperOptions: poolOptionsFromEnv({ envPrefix: 'upper', processEnv: process.env }),
});

module.exports = {
	main,
	setupOverlay,
	poolOptionsFromEnv,
	substitute,
};
