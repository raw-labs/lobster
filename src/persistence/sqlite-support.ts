type Version = readonly [number, number, number];

function parseVersion(value: string | undefined): Version | null {
	const match = /^(\d+)\.(\d+)\.(\d+)(?:\D.*)?$/.exec(value ?? "");
	return match ? [Number(match[1]), Number(match[2]), Number(match[3])] : null;
}

/** Keep file storage usable on older Node releases without loading node:sqlite. */
export function assertSqliteRuntime(
	nodeVersion = process.versions.node,
	sqliteVersion = process.versions.sqlite,
): void {
	const node = parseVersion(nodeVersion);
	if (
		!node ||
		node[0] < 22 ||
		(node[0] === 22 && node[1] < 13) ||
		(node[0] === 23 && node[1] < 4)
	) {
		throw new Error("SQLite backend requires Node 22.13+, 23.4+, or 24+");
	}

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
			`SQLite backend requires a runtime with the WAL-reset fix (SQLite 3.51.3+, or the 3.50.7/3.44.6 backports); found ${sqliteVersion ?? "unknown"}. Upgrade Node or use file storage.`,
		);
	}
}
