USE control;
-- An immutable admission decision, NOT the coupon grant or distributed ACID.
CREATE TABLE IF NOT EXISTS transaction_events (
 id CHAR(64) PRIMARY KEY, decision VARCHAR(16) NOT NULL,
 event_json JSON NOT NULL, trace_json JSON NOT NULL,
 created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB;
