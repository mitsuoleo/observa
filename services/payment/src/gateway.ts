export type Authorization = "APPROVED" | "REJECTED";
export type CircuitState = "closed" | "open" | "half_open";

type Options = {
  failFirst?: number;
  maxRetries?: number;
  baseDelayMs?: number;
  failureThreshold?: number;
  resetAfterMs?: number;
  now?: () => number;
  sleep?: (milliseconds: number) => Promise<void>;
};

function bounded(name: string, value: number, minimum: number, maximum: number): number {
  if (!Number.isInteger(value) || value < minimum || value > maximum) {
    throw new Error(`${name} must be an integer between ${minimum} and ${maximum}`);
  }
  return value;
}

/** Local fault simulator. A successful authorization is cached by order ID. */
export class SyntheticPaymentGateway {
  private readonly failFirst: number;
  private readonly maxRetries: number;
  private readonly baseDelayMs: number;
  private readonly failureThreshold: number;
  private readonly resetAfterMs: number;
  private readonly now: () => number;
  private readonly sleep: (milliseconds: number) => Promise<void>;
  private state: CircuitState = "closed";
  private openedAt = 0;
  private consecutiveFailures = 0;
  private attempts = 0;
  private failures = 0;
  private results = new Map<string, Authorization>();

  constructor(options: Options = {}) {
    this.failFirst = bounded("failFirst", options.failFirst ?? 0, 0, 1000);
    this.maxRetries = bounded("maxRetries", options.maxRetries ?? 2, 0, 10);
    this.baseDelayMs = bounded("baseDelayMs", options.baseDelayMs ?? 50, 0, 60000);
    this.failureThreshold = bounded("failureThreshold", options.failureThreshold ?? 3, 1, 100);
    this.resetAfterMs = bounded("resetAfterMs", options.resetAfterMs ?? 1000, 1, 300000);
    this.now = options.now ?? Date.now;
    this.sleep = options.sleep ?? (milliseconds => new Promise(resolve => setTimeout(resolve, milliseconds)));
  }

  snapshot() {
    return { state: this.state, attempts: this.attempts, failures: this.failures,
      authorizations: this.results.size, consecutiveFailures: this.consecutiveFailures };
  }

  async authorize(orderId: string, desired: Authorization): Promise<Authorization> {
    const cached = this.results.get(orderId);
    if (cached) return cached;
    if (this.state === "open") {
      if (this.now() - this.openedAt < this.resetAfterMs) throw new Error("payment gateway circuit open");
      this.state = "half_open";
    }
    for (let retry = 0; retry <= this.maxRetries; retry++) {
      this.attempts++;
      if (this.attempts > this.failFirst) {
        this.results = new Map(this.results).set(orderId, desired);
        this.consecutiveFailures = 0;
        this.state = "closed";
        return desired;
      }
      this.failures++;
      this.consecutiveFailures++;
      if (this.state === "half_open" || this.consecutiveFailures >= this.failureThreshold) {
        this.state = "open";
        this.openedAt = this.now();
        throw new Error("payment gateway circuit open");
      }
      if (retry === this.maxRetries) throw new Error("payment gateway transient failure");
      await this.sleep(this.baseDelayMs * 2 ** retry);
    }
    throw new Error("payment gateway retries exhausted");
  }
}
