// Standalone migration script: npm run migrate

import { loadConfig } from "./config.js";
import { Storage } from "./storage.js";
import { log } from "./logger.js";

async function main() {
  const config = loadConfig();
  const storage = new Storage(config.postgresUrl, config.pgPoolMax);

  log("info", "Running migration...");
  await storage.migrate();
  log("info", "Migration complete");

  await storage.close();
}

main().catch((err) => {
  console.error("Migration failed:", err);
  process.exit(1);
});
