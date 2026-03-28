CREATE DATABASE IF NOT EXISTS ecommerce;
USE ecommerce;

-- ============================================
-- CUSTOMERS TABLE
-- ============================================
CREATE TABLE customers (
    customer_id INT AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    phone VARCHAR(20),
    address VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- ============================================
-- PRODUCTS TABLE
-- ============================================
CREATE TABLE products (
    product_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    stock_quantity INT NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- ============================================
-- ORDERS TABLE
-- ============================================
CREATE TABLE orders (
    order_id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT NOT NULL,
    status ENUM('CREATED', 'PROCESSING', 'SHIPPED', 'DELIVERED', 'CANCELLED') NOT NULL DEFAULT 'CREATED',
    total_amount DECIMAL(12, 2) NOT NULL DEFAULT 0.00,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
) ENGINE=InnoDB;

-- ============================================
-- ORDER ITEMS TABLE
-- ============================================
CREATE TABLE order_items (
    order_item_id INT AUTO_INCREMENT PRIMARY KEY,
    order_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL DEFAULT 1,
    unit_price DECIMAL(10, 2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE,
    FOREIGN KEY (product_id) REFERENCES products(product_id)
) ENGINE=InnoDB;

-- ============================================
-- SEED PRODUCTS
-- ============================================
INSERT INTO products (name, category, price, stock_quantity) VALUES
-- Electronics
('Wireless Bluetooth Headphones', 'Electronics', 79.99, 150),
('USB-C Fast Charger', 'Electronics', 24.99, 300),
('Mechanical Keyboard', 'Electronics', 129.99, 80),
('27-inch 4K Monitor', 'Electronics', 349.99, 45),
('Portable SSD 1TB', 'Electronics', 89.99, 200),
-- Clothing
('Classic Fit Cotton T-Shirt', 'Clothing', 19.99, 500),
('Slim Fit Jeans', 'Clothing', 49.99, 250),
('Running Shoes', 'Clothing', 99.99, 120),
('Winter Puffer Jacket', 'Clothing', 149.99, 75),
-- Home
('Stainless Steel Water Bottle', 'Home', 14.99, 400),
('Memory Foam Pillow', 'Home', 39.99, 180),
('LED Desk Lamp', 'Home', 29.99, 220),
-- Sports
('Yoga Mat', 'Sports', 24.99, 300),
('Resistance Bands Set', 'Sports', 19.99, 350),
('Dumbbell Set 20kg', 'Sports', 59.99, 100);

-- ============================================
-- DEBEZIUM USER (for CDC in Phase 2)
-- ============================================
CREATE USER IF NOT EXISTS 'debezium'@'%' IDENTIFIED BY 'dbz';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium'@'%';
FLUSH PRIVILEGES;
