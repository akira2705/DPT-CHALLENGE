CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    price NUMERIC(10,2) NOT NULL DEFAULT 0,
    category TEXT,
    updated_at TIMESTAMPTZ DEFAULT now()
);

INSERT INTO products (name, price, category) VALUES
('Mouse', 499.00, 'electronics'),
('Notebook', 55.50, 'stationery'),
('Coffee', 120.00, 'beverage');
