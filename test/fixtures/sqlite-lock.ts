import { DatabaseSync } from "node:sqlite";

const db = new DatabaseSync(process.argv[2]);
db.exec("BEGIN IMMEDIATE");
process.stdout.write("locked\n");
setTimeout(() => {
	db.exec("COMMIT");
	db.close();
}, 250);
