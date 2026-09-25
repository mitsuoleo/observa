CREATE TABLE IF NOT EXISTS synthetic_outbox (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id uuid NOT NULL UNIQUE,
    envelope jsonb NOT NULL CHECK (jsonb_typeof(envelope) = 'object'),
    carrier jsonb NOT NULL CHECK (jsonb_typeof(carrier) = 'object'),
    created_at timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz,
    published_partition integer,
    published_offset bigint
);

CREATE INDEX IF NOT EXISTS synthetic_outbox_pending_idx
    ON synthetic_outbox (id) WHERE published_at IS NULL;

CREATE TABLE IF NOT EXISTS processed_events (
    event_id uuid PRIMARY KEY,
    processed_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS synthetic_effects (
    event_id uuid PRIMARY KEY REFERENCES processed_events (event_id),
    correlation_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
