import { SyntheticPaymentGateway } from "./gateway.js";

const delays: number[] = [];
const retry = new SyntheticPaymentGateway({ failFirst: 2, maxRetries: 2, baseDelayMs: 50,
  sleep: async milliseconds => { delays.push(milliseconds); } });
const authorized = await retry.authorize("retry-order", "APPROVED");
if (authorized !== "APPROVED" || delays.join(",") !== "50,100") throw new Error("retry scenario failed");

let clock = 0;
const breaker = new SyntheticPaymentGateway({ failFirst: 3, maxRetries: 1, failureThreshold: 2,
  resetAfterMs: 100, now: () => clock, sleep: async () => undefined });
const transitions: string[] = [];
for (const step of [0, 0, 101, 202]) {
  clock = step;
  try { await breaker.authorize("breaker-order", "APPROVED"); }
  catch { /* Expected transient/open-circuit faults are captured below. */ }
  transitions.push(breaker.snapshot().state);
}
if (transitions.join(",") !== "open,open,open,closed") throw new Error("breaker scenario failed");
process.stdout.write(JSON.stringify({ retry: { delays_ms: delays, ...retry.snapshot() },
  breaker: { transitions, ...breaker.snapshot() } }, null, 2) + "\n");
