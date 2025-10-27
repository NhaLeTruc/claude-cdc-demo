-- CDC Demo Database Initialization for MySQL

USE cdcdb;

-- Create products table
CREATE TABLE IF NOT EXISTS products (
    product_id INT AUTO_INCREMENT PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    description TEXT,
    category VARCHAR(100),
    price DECIMAL(10, 2) NOT NULL,
    stock_quantity INT DEFAULT 0,
    metadata JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_category (category),
    INDEX idx_price (price)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Create inventory_transactions table
CREATE TABLE IF NOT EXISTS inventory_transactions (
    transaction_id INT AUTO_INCREMENT PRIMARY KEY,
    product_id INT NOT NULL,
    transaction_type ENUM('IN', 'OUT', 'ADJUSTMENT') NOT NULL,
    quantity INT NOT NULL,
    previous_quantity INT NOT NULL,
    new_quantity INT NOT NULL,
    reason VARCHAR(255),
    transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_product_id (product_id),
    INDEX idx_transaction_type (transaction_type),
    INDEX idx_transaction_date (transaction_date),
    FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Create suppliers table
CREATE TABLE IF NOT EXISTS suppliers (
    supplier_id INT AUTO_INCREMENT PRIMARY KEY,
    supplier_name VARCHAR(255) NOT NULL,
    contact_name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(20),
    address TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_email (email)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Grant necessary permissions for CDC
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdcuser'@'%';
FLUSH PRIVILEGES;

-- Insert sample seed data
INSERT INTO products (product_name, description, category, price, stock_quantity, metadata) VALUES
    ('Desktop Computer', 'High-end desktop computer', 'Electronics', 1499.99, 30, '{"brand": "TechCorp", "warranty": "3 years"}'),
    ('Monitor', '27-inch 4K monitor', 'Electronics', 399.99, 75, '{"brand": "ViewMax", "resolution": "4K"}'),
    ('Office Chair', 'Ergonomic office chair', 'Furniture', 299.99, 100, '{"color": "black", "adjustable": true}')
ON DUPLICATE KEY UPDATE product_name=product_name;

INSERT INTO suppliers (supplier_name, contact_name, email, phone) VALUES
    ('TechSupply Co', 'Alice Brown', 'alice@techsupply.com', '555-1001'),
    ('Office Furniture Inc', 'Bob White', 'bob@officefurniture.com', '555-1002')
ON DUPLICATE KEY UPDATE supplier_name=supplier_name;
