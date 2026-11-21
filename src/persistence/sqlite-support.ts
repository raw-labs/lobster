type Version = readonly [number, number, number];

function parseVersion(value: string | undefined): Version | null {
	const match = /^(\d+)\.(\d+)\.(\d+)(?:\D.*)?$/.exec(value ?? "");
	return match ? [Number(match[1]), Number(match[2]), Number(match[3])] : null;
}

/**
 * Run state lives in SQLite (`node:sqlite`). Same floor as OpenClaw: Node 24.16+ on 24
 * or 26.1+. Earlier releases' `node:sqlite` truncates TEXT at embedded NULs (fixed in
 * 24.16.0 and 26.1.0), and Node 25 is not supported.
 */
export function assertSqliteNodeSupport(nodeVersion = process.versions.node): void {
	const node = parseVersion(nodeVersion);
	const supported =
		node &&
		((node[0] === 24 && node[1] >= 16) || (node[0] === 26 && node[1] >= 1) || node[0] >= 27);
	if (!supported) {
		throw new Error(
			`Lobster requires Node >=24.16.0 <25 or >=26.1.0 for its run store (node:sqlite); found ${nodeVersion}`,
		);
	}
}

/** Qualify the library used by the worker connection, before enabling WAL. */
export function assertSqliteRuntime(nodeVersion: string, sqliteVersion: string | undefined): void {
	assertSqliteNodeSupport(nodeVersion);
	const sqlite = parseVersion(sqliteVersion);
	const patched =
		sqlite &&
		(sqlite[0] > 3 ||
			(sqlite[0] === 3 &&
				(sqlite[1] > 51 ||
					(sqlite[1] === 51 && sqlite[2] >= 3) ||
					(sqlite[1] === 50 && sqlite[2] >= 7) ||
					(sqlite[1] === 44 && sqlite[2] >= 6))));
	if (!patched) {
		throw new Error(
			`SQLite backend requires a runtime with the WAL-reset fix (SQLite 3.51.3+, or the 3.50.7/3.44.6 backports); found ${sqliteVersion ?? "unknown"}. Upgrade Node.`,
		);
	}
}
