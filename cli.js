#!/usr/bin/env node

require('make-promises-safe'); // eslint-disable-line import/no-unassigned-import

const { main } = require('.');

(async () => {
	const { tables } = await main();

	tables.forEach(table => {
		if (table.readOnly) {
			console.warn('Table will be read-only:', [ table.schemaName, table.tableName ].join('.'));
		}
	});
})();
