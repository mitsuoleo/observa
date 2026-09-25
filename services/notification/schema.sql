CREATE TABLE IF NOT EXISTS processed_events (
  event_id uuid PRIMARY KEY,
  processed_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS notifications (
  event_id uuid PRIMARY KEY REFERENCES processed_events(event_id),
  order_id uuid NOT NULL,
  event_type text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
