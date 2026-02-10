-- GoArchive Test Fixtures: Customer/Orders Entity Model
-- Database: goarchive_test
-- Purpose: Integration test data for archiving operations

DROP DATABASE IF EXISTS goarchive_test;
CREATE DATABASE goarchive_test;
USE goarchive_test;

-- Root table: customers
CREATE TABLE customers (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_created (created_at)
) ENGINE=InnoDB;

-- Child table: orders (1-N from customers)
CREATE TABLE orders (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    total DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    status ENUM('pending', 'completed', 'cancelled') DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_customer (customer_id),
    INDEX idx_status (status),
    INDEX idx_created (created_at),
    FOREIGN KEY (customer_id) REFERENCES customers(id)
) ENGINE=InnoDB;

-- Grandchild table: order_items (1-N from orders)
CREATE TABLE order_items (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    order_id BIGINT NOT NULL,
    product VARCHAR(100) NOT NULL,
    quantity INT NOT NULL DEFAULT 1,
    price DECIMAL(10,2) NOT NULL,
    INDEX idx_order (order_id),
    FOREIGN KEY (order_id) REFERENCES orders(id)
) ENGINE=InnoDB;

-- Grandchild table: order_payments (1-N from orders)
CREATE TABLE order_payments (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    order_id BIGINT NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    method ENUM('credit_card', 'paypal', 'bank_transfer') NOT NULL,
    paid_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_order (order_id),
    FOREIGN KEY (order_id) REFERENCES orders(id)
) ENGINE=InnoDB;

-- ============================================================================
-- Seed Data: 10 customers, ~30 orders, ~75 items, ~30 payments
-- ============================================================================

-- Customers (IDs 1-10)
-- Old customers (1-5): created_at < '2024-01-01' - archive candidates
-- New customers (6-10): created_at >= '2024-01-01' - should NOT be archived
INSERT INTO customers (id, name, email, created_at) VALUES
(1, 'Alice Johnson', 'alice@example.com', '2023-01-15 10:00:00'),
(2, 'Bob Smith', 'bob@example.com', '2023-02-20 14:30:00'),
(3, 'Carol Williams', 'carol@example.com', '2023-03-10 09:15:00'),
(4, 'Dave Brown', 'dave@example.com', '2023-04-05 16:45:00'),
(5, 'Eve Davis', 'eve@example.com', '2023-05-01 11:00:00'),
(6, 'Frank Miller', 'frank@example.com', '2024-01-10 08:00:00'),
(7, 'Grace Wilson', 'grace@example.com', '2024-02-15 13:20:00'),
(8, 'Hank Moore', 'hank@example.com', '2024-03-20 10:30:00'),
(9, 'Iris Taylor', 'iris@example.com', '2024-04-25 15:00:00'),
(10, 'Jack Anderson', 'jack@example.com', '2024-05-30 09:45:00');

-- Orders: 3 per old customer (15 old orders), 2 per new customer (10 new orders)
INSERT INTO orders (id, customer_id, total, status, created_at) VALUES
-- Old customers' orders (archive candidates)
(101, 1, 150.00, 'completed', '2023-01-20 12:00:00'),
(102, 1, 75.50, 'completed', '2023-02-15 14:00:00'),
(103, 1, 200.00, 'cancelled', '2023-03-10 10:00:00'),
(104, 2, 320.00, 'completed', '2023-03-01 09:00:00'),
(105, 2, 45.00, 'completed', '2023-04-20 16:00:00'),
(106, 2, 125.00, 'pending', '2023-05-10 11:00:00'),
(107, 3, 890.00, 'completed', '2023-04-15 11:00:00'),
(108, 3, 120.00, 'completed', '2023-05-01 13:00:00'),
(109, 3, 67.00, 'cancelled', '2023-06-10 10:00:00'),
(110, 4, 55.00, 'completed', '2023-05-10 15:00:00'),
(111, 4, 430.00, 'completed', '2023-06-15 10:00:00'),
(112, 4, 88.50, 'pending', '2023-07-01 14:00:00'),
(113, 5, 67.50, 'completed', '2023-07-20 14:00:00'),
(114, 5, 210.00, 'completed', '2023-08-15 09:00:00'),
(115, 5, 95.00, 'cancelled', '2023-09-01 16:00:00'),
-- New customers' orders (should NOT be archived)
(201, 6, 99.00, 'completed', '2024-01-15 10:00:00'),
(202, 6, 150.00, 'pending', '2024-02-20 12:00:00'),
(203, 7, 275.00, 'completed', '2024-03-10 09:00:00'),
(204, 7, 88.00, 'completed', '2024-04-01 15:00:00'),
(205, 8, 180.00, 'completed', '2024-04-05 11:00:00'),
(206, 8, 320.00, 'pending', '2024-05-10 13:00:00'),
(207, 9, 340.00, 'completed', '2024-05-01 14:00:00'),
(208, 9, 125.00, 'pending', '2024-06-05 10:00:00'),
(209, 10, 95.00, 'completed', '2024-06-15 16:00:00'),
(210, 10, 200.00, 'completed', '2024-07-01 11:00:00');

-- Order items: 2-3 per order
INSERT INTO order_items (order_id, product, quantity, price) VALUES
-- Old orders' items
(101, 'Widget A', 2, 50.00), (101, 'Widget B', 1, 50.00),
(102, 'Gadget X', 1, 75.50),
(103, 'Widget A', 4, 50.00),
(104, 'Premium Pack', 1, 320.00),
(105, 'Widget C', 3, 15.00),
(106, 'Widget A', 2, 50.00), (106, 'Widget B', 1, 25.00),
(107, 'Deluxe Set', 1, 890.00),
(108, 'Widget A', 2, 50.00), (108, 'Widget B', 1, 20.00),
(109, 'Gadget Y', 1, 67.00),
(110, 'Gadget Y', 1, 55.00),
(111, 'Premium Pack', 1, 320.00), (111, 'Widget C', 2, 55.00),
(112, 'Widget B', 3, 29.50),
(113, 'Widget C', 3, 22.50),
(114, 'Widget A', 4, 50.00), (114, 'Widget C', 1, 10.00),
(115, 'Gadget X', 1, 95.00),
-- New orders' items
(201, 'Widget A', 1, 99.00),
(202, 'Gadget X', 2, 75.00),
(203, 'Deluxe Set', 1, 275.00),
(204, 'Widget C', 4, 22.00),
(205, 'Premium Pack', 1, 180.00),
(206, 'Widget A', 5, 64.00),
(207, 'Premium Pack', 1, 340.00),
(208, 'Widget B', 5, 25.00),
(209, 'Gadget Y', 1, 95.00),
(210, 'Widget A', 4, 50.00);

-- Order payments: 1 per completed order
INSERT INTO order_payments (order_id, amount, method, paid_at) VALUES
-- Old orders' payments
(101, 150.00, 'credit_card', '2023-01-20 12:05:00'),
(102, 75.50, 'paypal', '2023-02-15 14:10:00'),
(104, 320.00, 'credit_card', '2023-03-01 09:15:00'),
(105, 45.00, 'bank_transfer', '2023-04-20 16:30:00'),
(107, 890.00, 'credit_card', '2023-04-15 11:20:00'),
(108, 120.00, 'paypal', '2023-05-01 13:15:00'),
(110, 55.00, 'paypal', '2023-05-10 15:10:00'),
(111, 430.00, 'credit_card', '2023-06-15 10:30:00'),
(113, 67.50, 'bank_transfer', '2023-07-20 14:15:00'),
(114, 210.00, 'credit_card', '2023-08-15 09:10:00'),
-- New orders' payments
(201, 99.00, 'credit_card', '2024-01-15 10:15:00'),
(203, 275.00, 'paypal', '2024-03-10 09:20:00'),
(204, 88.00, 'credit_card', '2024-04-01 15:10:00'),
(205, 180.00, 'credit_card', '2024-04-05 11:10:00'),
(207, 340.00, 'paypal', '2024-05-01 14:20:00'),
(209, 95.00, 'bank_transfer', '2024-06-15 16:15:00'),
(210, 200.00, 'credit_card', '2024-07-01 11:15:00');

-- ============================================================================
-- Row Count Summary
-- ============================================================================
-- Customers: 10 total (5 old, 5 new)
-- Orders: 25 total (15 old, 10 new)
-- Order Items: 31 total (20 old, 11 new)
-- Order Payments: 17 total (10 old, 7 new)
--
-- Archive candidates (WHERE customers.created_at < '2024-01-01'):
--   5 customers (IDs 1-5)
--   15 orders (IDs 101-115)
--   20 order_items
--   10 order_payments
