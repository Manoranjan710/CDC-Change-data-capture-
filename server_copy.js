const mysql = require('mysql2/promise');
const {createClient } = require('redis');

// Configuration - adjust as needed
const MYSQL_CONFIG = {
    host : 'localhost',
    port : 3306,
    user : 'root',
    password : 'Codot@12345',
    database : 'cloth_store',
};

const REDIS_STREAM = 'cloth_store_changes';
const POLL_INTERVAL_MS = 1000;

let stopRequested = false;

function sortedJson(obj) {
	if (obj === null || typeof obj !== 'object') return obj;
	if (Array.isArray(obj)) return obj.map(sortedJson);
	const keys = Object.keys(obj).sort();
	const out = {};
	for (const k of keys) out[k] = sortedJson(obj[k]);
	return out;
}

function rowToStableString(row) {
	return JSON.stringify(sortedJson(row));
}

async function getPrimaryKeyColumns(conn, schema, table) {
	const [rows] = await conn.execute(
		`SELECT k.COLUMN_NAME
		 FROM information_schema.table_constraints t
		 JOIN information_schema.key_column_usage k
			 USING(constraint_name,table_schema,table_name)
		 WHERE t.constraint_type='PRIMARY KEY'
			 AND k.table_schema=?
			 AND k.table_name=?
		 ORDER BY k.ordinal_position`,
		[schema, table]
	);
	return rows.map(r => r.COLUMN_NAME);
}


function makeKeyFromRow(row, pkCols) {
	if (pkCols && pkCols.length > 0) {
		return pkCols.map(c => String(row[c])).join('::');
	}
	// fallback: use full row stable JSON
	return rowToStableString(row);
}


async function main() {
	const redis = createClient();
	await redis.connect();

	const conn = await mysql.createConnection(MYSQL_CONFIG);
	const schema = MYSQL_CONFIG.database;
	const table = 'shirts';

	console.log(`✅ Connected to MySQL ${schema} and Redis`);

	const pkCols = await getPrimaryKeyColumns(conn, schema, table);
	if (pkCols.length) {
		console.log('Primary key columns:', pkCols.join(','));
	} else {
		console.log('No primary key detected — using full-row JSON as key');
	}
	// load initial snapshot
	const [initialRows] = await conn.execute(`SELECT * FROM \`${schema}\`.\`${table}\``);
	let prevMap = new Map();
	for (const r of initialRows) prevMap.set(makeKeyFromRow(r, pkCols), r);

	console.log(`Listening for changes in ${schema}.${table} (poll every ${POLL_INTERVAL_MS}ms)`);
	while (!stopRequested) {
		try {
			const [rows] = await conn.execute(`SELECT * FROM \`${schema}\`.\`${table}\``);
			const newMap = new Map();
			for (const r of rows) newMap.set(makeKeyFromRow(r, pkCols), r);
			// Detect inserts and updates
			for (const [key, newRow] of newMap.entries()) {
				if (!prevMap.has(key)) {
					// insert
					const data = {
						db: schema,
						table,
						eventName: 'writerows',
						row: newRow,
						timestamp: new Date().toISOString(),
					};
					await redis.xAdd(REDIS_STREAM, '*', { change: JSON.stringify(data) });
					console.log('📩 Insert detected and pushed to Redis:', key);
				} else {
					const oldRow = prevMap.get(key);
					const before = rowToStableString(oldRow);
					const after = rowToStableString(newRow);
					if (before !== after) {
						const data = {
							db: schema,
							table,
							eventName: 'updaterows',
							row: { before: oldRow, after: newRow },
							timestamp: new Date().toISOString(),
						};
						await redis.xAdd(REDIS_STREAM, '*', { change: JSON.stringify(data) });
						console.log('🔁 Update detected and pushed to Redis:', key);
					}
				}
			}
			// Detect deletes
			for (const [key, oldRow] of prevMap.entries()) {
				if (!newMap.has(key)) {
					const data = {
						db: schema,
						table,
						eventName: 'deleterows',
						row: oldRow,
						timestamp: new Date().toISOString(),
					};
					await redis.xAdd(REDIS_STREAM, '*', { change: JSON.stringify(data) });
					console.log('❌ Delete detected and pushed to Redis:', key);
				}
			}
			prevMap = newMap;
		} catch (err) {
			console.error('Error during polling loop:', err);
		}

		await new Promise(r => setTimeout(r, POLL_INTERVAL_MS));
	}
	await conn.end();
	await redis.disconnect();
}
process.on('SIGINT', () => {
	console.log('Stopping...');
	stopRequested = true;
});

main().catch(err => {
	console.error(err);
	process.exit(1);
});
