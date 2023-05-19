CREATE TABLE IF NOT EXISTS message_outbox
(
    id                 SERIAL,
    class              TEXT NOT NULL,
    topic              TEXT NOT NULL,
    serialized         BYTEA,
    creation_date_time TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT message_outbox_pk PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS shedlock
(
    "name"     VARCHAR(64)  NOT NULL,
    lock_until TIMESTAMP    NULL,
    locked_at  TIMESTAMP    NULL,
    locked_by  VARCHAR(255) NULL,
    CONSTRAINT shedlock_pk PRIMARY KEY (name)
);
