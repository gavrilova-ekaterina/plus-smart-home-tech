CREATE TABLE IF NOT EXISTS inventory_items (
    id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_id        BIGINT  NOT NULL UNIQUE,
    quantity          INTEGER NOT NULL,
    reserved_quantity INTEGER NOT NULL DEFAULT 0,
    version           BIGINT  NOT NULL DEFAULT 0
);
