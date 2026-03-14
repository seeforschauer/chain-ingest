// Minimal structured logger — no dependencies.
// At 120 chains, unfiltered debug logs are a firehose.

type LogLevel = "debug" | "info" | "warn" | "error";

const LEVEL_RANK: Record<LogLevel, number> = {
  debug: 0,
  info: 1,
  warn: 2,
  error: 3,
};

let minLevel: LogLevel = "info";

export function setLogLevel(level: LogLevel): void {
  minLevel = level;
}

export function log(
  level: LogLevel,
  msg: string,
  data?: Record<string, unknown>
): void {
  if (LEVEL_RANK[level] < LEVEL_RANK[minLevel]) return;

  const entry = {
    ts: new Date().toISOString(),
    level,
    msg,
    ...data,
  };
  const line = JSON.stringify(entry);
  if (level === "error") {
    console.error(line);
  } else {
    console.log(line);
  }
}
