import { describe, expect, it } from "vitest";
import { decideReservation } from "../src/decision.js";

const item = { product_id: "11111111-1111-1111-1111-111111111111", quantity: 2, unit_price: 49.9 };

describe("inventory decision", () => {
  it("reserves available stock and keeps the public payload shape", () => {
    expect(decideReservation("catalog", [item], new Map([[item.product_id, 3]]))).toEqual({
      reserved: true,
      payload: { items: [{ product_id: item.product_id, quantity: 2 }] },
    });
  });
  it("rejects unavailable stock without writing any reservation", () => {
    expect(decideReservation("catalog", [item], new Map([[item.product_id, 1]]))).toMatchObject({
      reserved: false, payload: { reason: "insufficient_stock" },
    });
    expect(decideReservation("unavailable", [item], new Map([[item.product_id, 100]]))).toMatchObject({
      reserved: false, payload: { reason: "simulated_unavailable" },
    });
    expect(decideReservation("reserve", [item], new Map())).toMatchObject({
      reserved: false, payload: { reason: "insufficient_stock" },
    });
  });
  it("rejects malformed quantities before database effects", () => {
    expect(() => decideReservation("catalog", [{ ...item, quantity: 0 }], new Map())).toThrow();
  });
  it("combines duplicate product lines into one reservation quantity", () => {
    expect(decideReservation("catalog", [item, { ...item, quantity: 1 }], new Map([[item.product_id, 3]]))).toEqual({
      reserved: true,
      payload: { items: [{ product_id: item.product_id, quantity: 3 }] },
    });
  });
});
