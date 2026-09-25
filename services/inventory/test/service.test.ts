import { describe, expect, it } from "vitest";
import { paymentPayload } from "../src/service.js";

describe("Inventory input boundary", () => {
  const orderId = "11111111-1111-4111-8111-111111111111";
  const event = {
    event_id: "22222222-2222-4222-8222-222222222222",
    event_type: "payment.approved",
    version: 1,
    correlation_id: orderId,
    occurred_at: "2026-09-24T00:00:00Z",
    payload: {
      order_id: orderId,
      items: [{ product_id: "11111111-1111-1111-1111-111111111111", quantity: 1 }],
      simulate: { stock: "reserve" },
    },
  };

  it("accepts the seeded product UUID used by the MVP", () => {
    expect(paymentPayload(event)).toMatchObject({ orderId, items: event.payload.items });
  });

  it("rejects malformed product identifiers as permanent records", () => {
    expect(() => paymentPayload({ ...event, payload: {
      ...event.payload, items: [{ product_id: "bad", quantity: 1 }],
    } })).toThrow("invalid inventory item");
  });
});
