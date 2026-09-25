CREATE TABLE IF NOT EXISTS products (
  id UUID PRIMARY KEY, sku VARCHAR(64) NOT NULL UNIQUE, name VARCHAR(120) NOT NULL,
  quantity INT NOT NULL CHECK (quantity >= 0), unit_price NUMERIC(12,2) NOT NULL
);
CREATE TABLE IF NOT EXISTS reservations (
  id UUID PRIMARY KEY, order_id UUID NOT NULL, product_id UUID NOT NULL REFERENCES products(id),
  quantity INT NOT NULL CHECK (quantity > 0), created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (order_id, product_id)
);
CREATE TABLE IF NOT EXISTS processed_events (
  event_id UUID PRIMARY KEY, processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS outbox_events (
  id BIGSERIAL PRIMARY KEY, event_id UUID NOT NULL UNIQUE, event_type VARCHAR(80) NOT NULL,
  payload JSONB NOT NULL, carrier JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), published_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_inventory_outbox_unpublished ON outbox_events (id) WHERE published_at IS NULL;
INSERT INTO products (id,sku,name,quantity,unit_price) VALUES
('11111111-1111-1111-1111-111111111111','WIDGET','Widget',100,49.90),
('22222222-2222-2222-2222-222222222222','GADGET','Gadget',8,19.50),
('33333333-3333-3333-3333-333333333333','RARE','Rare item',0,999.00)
ON CONFLICT (id) DO NOTHING;
