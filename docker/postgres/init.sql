-- CDC Demo Database Initialization for PostgreSQL

-- Create customers table
CREATE TABLE IF NOT EXISTS customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    phone VARCHAR(20),
    address TEXT,
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    country VARCHAR(100) DEFAULT 'USA',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create orders table
CREATE TABLE IF NOT EXISTS orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(customer_id),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_amount DECIMAL(10, 2) NOT NULL,
    status VARCHAR(50) DEFAULT 'pending',
    shipping_address TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create products table
CREATE TABLE IF NOT EXISTS products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    description TEXT,
    category VARCHAR(100),
    price DECIMAL(10, 2) NOT NULL,
    stock_quantity INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create order_items table
CREATE TABLE IF NOT EXISTS order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES orders(order_id),
    product_id INTEGER NOT NULL REFERENCES products(product_id),
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_customers_email ON customers(email);
CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON order_items(product_id);
CREATE INDEX IF NOT EXISTS idx_products_category ON products(category);

-- Create updated_at trigger function
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Add triggers for updated_at
CREATE TRIGGER update_customers_updated_at BEFORE UPDATE ON customers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_orders_updated_at BEFORE UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_products_updated_at BEFORE UPDATE ON products
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Grant necessary permissions for CDC user
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO ${POSTGRES_USER};
GRANT USAGE ON SCHEMA public TO ${POSTGRES_USER};
GRANT SELECT ON ALL TABLES IN SCHEMA public TO ${POSTGRES_USER};
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO ${POSTGRES_USER};

-- Create replication slot for Debezium (will be created by Debezium connector)
-- SELECT pg_create_logical_replication_slot('debezium_slot', 'pgoutput');

-- Insert sample seed data
INSERT INTO customers (first_name, last_name, email, phone, city, state, country) VALUES
    ('John', 'Doe', 'john.doe@example.com', '555-0001', 'New York', 'NY', 'USA'),
    ('Jane', 'Smith', 'jane.smith@example.com', '555-0002', 'Los Angeles', 'CA', 'USA'),
    ('Bob', 'Johnson', 'bob.johnson@example.com', '555-0003', 'Chicago', 'IL', 'USA')
ON CONFLICT (email) DO NOTHING;

INSERT INTO products (product_name, description, category, price, stock_quantity) VALUES
    ('Laptop', 'High-performance laptop', 'Electronics', 999.99, 50),
    ('Mouse', 'Wireless mouse', 'Electronics', 29.99, 200),
    ('Keyboard', 'Mechanical keyboard', 'Electronics', 89.99, 150)
ON CONFLICT DO NOTHING;

-- ============================================================================
-- Schema Evolution Test Table
-- ============================================================================
-- This table is used to test schema evolution scenarios in CDC pipelines.
-- Tests include: ADD COLUMN, DROP COLUMN, ALTER TYPE, RENAME COLUMN
-- ============================================================================

CREATE TABLE IF NOT EXISTS schema_evolution_test (
    id SERIAL PRIMARY KEY,
    version INTEGER NOT NULL DEFAULT 1,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    status VARCHAR(50) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index
CREATE INDEX IF NOT EXISTS idx_schema_evolution_test_status ON schema_evolution_test(status);

-- Add trigger for updated_at
CREATE TRIGGER update_schema_evolution_test_updated_at BEFORE UPDATE ON schema_evolution_test
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Insert initial test data
INSERT INTO schema_evolution_test (version, name, description, status) VALUES
    (1, 'Test Record 1', 'Initial schema version', 'active'),
    (1, 'Test Record 2', 'Initial schema version', 'active'),
    (1, 'Test Record 3', 'Initial schema version', 'inactive')
ON CONFLICT DO NOTHING;

-- ============================================================================
-- Schema Evolution Test Scenarios
-- ============================================================================
-- The following SQL statements can be executed to test schema evolution.
-- These are commented out and should be run manually during testing.
-- ============================================================================

-- Scenario 1: ADD COLUMN (backward compatible)
-- ALTER TABLE schema_evolution_test ADD COLUMN email VARCHAR(255);
-- ALTER TABLE schema_evolution_test ADD COLUMN phone VARCHAR(20);
-- ALTER TABLE schema_evolution_test ADD COLUMN metadata JSONB DEFAULT '{}';

-- Scenario 2: DROP COLUMN (potentially breaking)
-- ALTER TABLE schema_evolution_test DROP COLUMN description;
-- ALTER TABLE schema_evolution_test DROP COLUMN phone;

-- Scenario 3: ALTER COLUMN TYPE (potentially breaking)
-- ALTER TABLE schema_evolution_test ALTER COLUMN status TYPE VARCHAR(100);
-- ALTER TABLE schema_evolution_test ALTER COLUMN version TYPE BIGINT;

-- Scenario 4: RENAME COLUMN (breaking for non-schema-aware consumers)
-- ALTER TABLE schema_evolution_test RENAME COLUMN name TO display_name;
-- ALTER TABLE schema_evolution_test RENAME COLUMN description TO description_text;

-- Scenario 5: ADD NOT NULL CONSTRAINT (breaking)
-- ALTER TABLE schema_evolution_test ALTER COLUMN email SET NOT NULL;

-- Scenario 6: ADD DEFAULT VALUE (backward compatible)
-- ALTER TABLE schema_evolution_test ALTER COLUMN status SET DEFAULT 'pending';

-- Scenario 7: ADD CHECK CONSTRAINT (potentially breaking)
-- ALTER TABLE schema_evolution_test ADD CONSTRAINT check_status
--     CHECK (status IN ('active', 'inactive', 'pending', 'deleted'));

-- ============================================================================
-- Schema Version Tracking Table
-- ============================================================================
-- This table tracks schema changes for auditing and rollback purposes
-- ============================================================================

CREATE TABLE IF NOT EXISTS schema_version_history (
    version_id SERIAL PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL,
    schema_version INTEGER NOT NULL,
    change_type VARCHAR(50) NOT NULL, -- ADD_COLUMN, DROP_COLUMN, ALTER_TYPE, RENAME_COLUMN
    change_description TEXT NOT NULL,
    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    executed_by VARCHAR(100) DEFAULT CURRENT_USER
);

-- Insert initial schema version
INSERT INTO schema_version_history (table_name, schema_version, change_type, change_description) VALUES
    ('schema_evolution_test', 1, 'CREATE_TABLE', 'Initial table creation with base schema')
ON CONFLICT DO NOTHING;
