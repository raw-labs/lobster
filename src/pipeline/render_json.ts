export function createJsonRenderer(stdout: NodeJS.WritableStream) {
	return {
		json(items: unknown) {
			stdout.write(JSON.stringify(items, null, 2));
			stdout.write("\n");
		},
		lines(lines: Iterable<unknown>) {
			for (const line of lines) stdout.write(String(line) + "\n");
		},
	};
}
