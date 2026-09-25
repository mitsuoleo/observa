export type Item = Readonly<{ product_id: string; quantity: number; unit_price?: number }>;
export type Mode = "catalog" | "reserve" | "unavailable";

export function decideReservation(mode: Mode, items: readonly Item[], available: ReadonlyMap<string, number>) {
  if (items.length === 0 || items.some(item => !Number.isInteger(item.quantity) || item.quantity <= 0)) {
    throw new Error("invalid inventory items");
  }
  const quantities = new Map<string, number>();
  for (const item of items) quantities.set(item.product_id, (quantities.get(item.product_id) ?? 0) + item.quantity);
  const enough = [...quantities].every(([id, requested]) => (available.get(id) ?? 0) >= requested);
  const reserved = mode !== "unavailable" && enough;
  return reserved
    ? { reserved: true as const, payload: { items: [...quantities].map(([product_id, quantity]) => ({product_id, quantity})) } }
    : { reserved: false as const, payload: { reason: mode === "unavailable" ? "simulated_unavailable" : "insufficient_stock", items: items.map(item => ({ ...item })) } };
}
