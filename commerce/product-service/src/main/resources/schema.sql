CREATE TABLE IF NOT EXISTS categories (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name        VARCHAR(255) NOT NULL,
    description VARCHAR(500)
);

CREATE TABLE IF NOT EXISTS products (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name        VARCHAR(255) NOT NULL,
    description VARCHAR(2000),
    price       NUMERIC(19, 2) NOT NULL,
    category_id BIGINT REFERENCES categories(id),
    image_url   VARCHAR(255),
    active      BOOLEAN NOT NULL DEFAULT TRUE
);
