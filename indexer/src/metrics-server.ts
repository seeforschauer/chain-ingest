// HTTP server exposing /metrics for Prometheus scraping.

import http from "node:http";
import type { Registry } from "prom-client";
import { log } from "./logger.js";

export function startMetricsServer(
  port: number,
  registry: Registry
): http.Server {
  const server = http.createServer(async (req, res) => {
    if (req.method === "GET" && req.url === "/metrics") {
      try {
        const metrics = await registry.metrics();
        res.writeHead(200, { "Content-Type": registry.contentType });
        res.end(metrics);
      } catch (err) {
        res.writeHead(500);
        res.end();
      }
      return;
    }
    res.writeHead(404);
    res.end();
  });

  server.listen(port, () => {
    log("info", "Metrics server listening", { port });
  });

  return server;
}
