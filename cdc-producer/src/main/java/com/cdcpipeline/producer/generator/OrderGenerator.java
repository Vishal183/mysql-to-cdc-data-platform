package com.cdcpipeline.producer.generator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class OrderGenerator {

    private static final Logger log = LoggerFactory.getLogger(OrderGenerator.class);
    private static final Map<String, String[]> STATUS_TRANSITIONS = Map.of(
            "CREATED", new String[]{"PROCESSING"},
            "PROCESSING", new String[]{"SHIPPED"},
            "SHIPPED", new String[]{"DELIVERED", "DELIVERED", "DELIVERED", "DELIVERED", "CANCELLED"} // 80% delivered
    );

    private final Random random = new Random();
    private final OrderItemGenerator orderItemGenerator = new OrderItemGenerator();

    public int createOrder(Connection conn, List<Integer> customerIds, List<Integer> productIds) throws SQLException {
        if (customerIds.isEmpty() || productIds.isEmpty()) {
            log.warn("Cannot create order: no customers or products available");
            return -1;
        }

        int customerId = customerIds.get(random.nextInt(customerIds.size()));

        conn.setAutoCommit(false);
        try {
            // Insert order with CREATED status
            String orderSql = "INSERT INTO orders (customer_id, status, total_amount) VALUES (?, 'CREATED', 0.00)";
            int orderId;
            try (PreparedStatement stmt = conn.prepareStatement(orderSql, Statement.RETURN_GENERATED_KEYS)) {
                stmt.setInt(1, customerId);
                stmt.executeUpdate();
                try (ResultSet keys = stmt.getGeneratedKeys()) {
                    keys.next();
                    orderId = keys.getInt(1);
                }
            }

            // Insert order items and compute total
            BigDecimal totalAmount = orderItemGenerator.insertOrderItems(conn, orderId, productIds);

            // Update order with computed total
            String updateSql = "UPDATE orders SET total_amount = ? WHERE order_id = ?";
            try (PreparedStatement stmt = conn.prepareStatement(updateSql)) {
                stmt.setBigDecimal(1, totalAmount);
                stmt.setInt(2, orderId);
                stmt.executeUpdate();
            }

            conn.commit();
            log.info("INSERT order #{}: customer={}, total=${}, status=CREATED", orderId, customerId, totalAmount);
            return orderId;

        } catch (SQLException e) {
            conn.rollback();
            throw e;
        } finally {
            conn.setAutoCommit(true);
        }
    }

    public boolean updateOrderStatus(Connection conn) throws SQLException {
        // Find a random order that can be advanced
        String selectSql = "SELECT order_id, status FROM orders WHERE status IN ('CREATED', 'PROCESSING', 'SHIPPED') ORDER BY RAND() LIMIT 1";

        try (PreparedStatement stmt = conn.prepareStatement(selectSql);
             ResultSet rs = stmt.executeQuery()) {

            if (!rs.next()) {
                log.info("No orders available for status update");
                return false;
            }

            int orderId = rs.getInt("order_id");
            String currentStatus = rs.getString("status");

            String[] nextStatuses = STATUS_TRANSITIONS.get(currentStatus);
            if (nextStatuses == null) {
                return false;
            }
            String newStatus = nextStatuses[random.nextInt(nextStatuses.length)];

            String updateSql = "UPDATE orders SET status = ? WHERE order_id = ?";
            try (PreparedStatement updateStmt = conn.prepareStatement(updateSql)) {
                updateStmt.setString(1, newStatus);
                updateStmt.setInt(2, orderId);
                updateStmt.executeUpdate();
            }

            log.info("UPDATE order #{}: {} -> {}", orderId, currentStatus, newStatus);
            return true;
        }
    }

    public boolean deleteCancelledOrder(Connection conn) throws SQLException {
        String selectSql = "SELECT order_id FROM orders WHERE status = 'CANCELLED' ORDER BY RAND() LIMIT 1";

        try (PreparedStatement stmt = conn.prepareStatement(selectSql);
             ResultSet rs = stmt.executeQuery()) {

            if (!rs.next()) {
                log.info("No cancelled orders to delete");
                return false;
            }

            int orderId = rs.getInt("order_id");

            // CASCADE DELETE will remove order_items automatically
            String deleteSql = "DELETE FROM orders WHERE order_id = ?";
            try (PreparedStatement deleteStmt = conn.prepareStatement(deleteSql)) {
                deleteStmt.setInt(1, orderId);
                deleteStmt.executeUpdate();
            }

            log.info("DELETE order #{} (CANCELLED) — cascaded to order_items", orderId);
            return true;
        }
    }
}
