CREATE TABLE IF NOT EXISTS orders (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_name  VARCHAR(255)   NOT NULL,
    customer_email VARCHAR(255)   NOT NULL,
    status         VARCHAR(50)    NOT NULL,
    total_price    NUMERIC(19, 2) NOT NULL,
    status_details VARCHAR(255),
    created_at     TIMESTAMP      NOT NULL
);

CREATE TABLE IF NOT EXISTS order_items (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id     BIGINT         NOT NULL REFERENCES orders (id),
    product_id   BIGINT         NOT NULL,
    product_name VARCHAR(255),
    quantity     INTEGER        NOT NULL,
    price        NUMERIC(19, 2)
);
