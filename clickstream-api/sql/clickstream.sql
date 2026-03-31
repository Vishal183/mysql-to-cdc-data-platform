USE ecommerce;

CREATE TABLE IF NOT EXISTS clickstream (
    event_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT NOT NULL,
    product_id INT NULL,
    event_type ENUM('VIEW', 'ADD_TO_CART', 'REMOVE_FROM_CART', 'WISHLIST', 'SEARCH') NOT NULL,
    search_query VARCHAR(255) NULL,
    session_id VARCHAR(36) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
) ENGINE=InnoDB;
