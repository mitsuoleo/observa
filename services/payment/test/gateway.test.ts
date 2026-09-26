import { describe, expect, it, vi } from "vitest";
import { SyntheticPaymentGateway } from "../src/gateway.js";

describe("synthetic payment gateway", () => {
  it("retries a bounded transient failure and caches one authorization per order", async () => {
    const sleep = vi.fn(async () => undefined);
    const gateway = new SyntheticPaymentGateway({ failFirst: 2, maxRetries: 2, baseDelayMs: 5, sleep });
    expect(await gateway.authorize("order-1", "APPROVED")).toBe("APPROVED");
    expect(await gateway.authorize("order-1", "APPROVED")).toBe("APPROVED");
    expect(gateway.snapshot()).toMatchObject({ attempts: 3, failures: 2, authorizations: 1, state: "closed" });
    expect(sleep.mock.calls).toHaveLength(2);
  });

  it("opens after repeated failures and probes again after cooldown", async () => {
    let now = 0;
    const gateway = new SyntheticPaymentGateway({ failFirst: 3, maxRetries: 1, failureThreshold: 2,
      resetAfterMs: 100, now: () => now, sleep: async () => undefined });
    await expect(gateway.authorize("order-1", "APPROVED")).rejects.toThrow("circuit open");
    expect(gateway.snapshot().state).toBe("open");
    await expect(gateway.authorize("order-2", "APPROVED")).rejects.toThrow("circuit open");
    expect(gateway.snapshot().attempts).toBe(2);
    now = 101;
    await expect(gateway.authorize("order-2", "APPROVED")).rejects.toThrow();
    expect(gateway.snapshot().state).toBe("open");
    now = 202;
    expect(await gateway.authorize("order-2", "APPROVED")).toBe("APPROVED");
    expect(gateway.snapshot().state).toBe("closed");
  });

  it("rejects unsafe configuration", () => {
    expect(() => new SyntheticPaymentGateway({ failFirst: -1 })).toThrow("failFirst");
    expect(() => new SyntheticPaymentGateway({ maxRetries: 99 })).toThrow("maxRetries");
  });
});
