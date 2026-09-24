import { it, expect, vi } from "vitest";
import { processRecord, STARTED, COMPLETED } from "../src/processor.js";
const p = {
  probe_id: "11111111-1111-4111-8111-111111111111",
  order_id: "22222222-2222-4222-8222-222222222222",
  step: "started",
  occurred_at: "2026-09-24T00:00:00Z",
};
const input = {
  topic: STARTED,
  partition: 0,
  message: {
    key: Buffer.from(p.order_id),
    value: Buffer.from(JSON.stringify(p)),
    offset: "9007199254740993",
  },
};
it("waits for publish acknowledgement then commits precise next offset", async () => {
  const order: string[] = [];
  const send = vi.fn(async () => {
    order.push("publish");
    return [{ partition: 1, baseOffset: "3" }];
  });
  const commit = vi.fn(async () => {
    order.push("commit");
  });
  await processRecord(input, { send, commit, log: vi.fn() });
  expect(order).toEqual(["publish", "commit"]);
  expect(commit).toHaveBeenCalledWith({
    topic: STARTED,
    partition: 0,
    offset: "9007199254740994",
  });
  expect(send.mock.calls[0][0].topic).toBe(COMPLETED);
});
it("does not commit failed publish", async () => {
  const commit = vi.fn();
  await expect(
    processRecord(input, {
      send: async () => {
        throw Error("broker");
      },
      commit,
      log: vi.fn(),
    }),
  ).rejects.toThrow("broker");
  expect(commit).not.toHaveBeenCalled();
});
it("does not publish or commit injected failure", async () => {
  const send = vi.fn(),
    commit = vi.fn();
  await expect(
    processRecord(input, {
      send,
      commit,
      log: vi.fn(),
      failProbeId: p.probe_id,
    }),
  ).rejects.toThrow("fault_before_publish");
  expect(send).not.toHaveBeenCalled();
  expect(commit).not.toHaveBeenCalled();
});
it("surfaces commit failure without success log", async () => {
  const log = vi.fn();
  await expect(
    processRecord(input, {
      send: async () => [],
      commit: async () => {
        throw Error("commit");
      },
      log,
    }),
  ).rejects.toThrow("commit");
  expect(log.mock.calls.map((c) => c[0])).not.toContain("process_end");
});
it("rejects wrong topic, step and missing payload", async () => {
  for (const value of [
    { ...input, topic: "wrong" },
    { ...input, message: { ...input.message, value: null } },
    {
      ...input,
      message: {
        ...input.message,
        value: Buffer.from(JSON.stringify({ ...p, step: "completed" })),
      },
    },
  ])
    await expect(
      processRecord(value, { send: vi.fn(), commit: vi.fn(), log: vi.fn() }),
    ).rejects.toThrow();
});
