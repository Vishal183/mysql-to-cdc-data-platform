package com.cdcpipeline.producer.generator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class OrderItemGenerator {

    private static final Logger log = LoggerFactory.getLogger(OrderItemGenerator.class);
    private final Random random = new Random();

    public BigDecimal insertOrderItems(Connection conn, int orderId, List<Integer> productIds) throws SQLException {
        int itemCount = random.nextInt(4) + 1; // 1-4 items per order
        List<Integer> shuffled = new ArrayList<>(productIds);
        Collections.shuffle(shuffled);
        List<Integer> selectedProducts = shuffled.subList(0, Math.min(itemCount, shuffled.size()));

        String insertSql = "INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (?, ?, ?, ?)";
        String priceSql = "SELECT price FROM products WHERE product_id = ?";

        BigDecimal totalAmount = BigDecimal.ZERO;

        for (int productId : selectedProducts) {
            BigDecimal unitPrice;
            try (PreparedStatement priceStmt = conn.prepareStatement(priceSql)) {
                priceStmt.setInt(1, productId);
                try (ResultSet rs = priceStmt.executeQuery()) {
                    rs.next();
                    unitPrice = rs.getBigDecimal("price");
                }
            }

            int quantity = random.nextInt(3) + 1; // 1-3 units

            try (PreparedStatement stmt = conn.prepareStatement(insertSql)) {
                stmt.setInt(1, orderId);
                stmt.setInt(2, productId);
                stmt.setInt(3, quantity);
                stmt.setBigDecimal(4, unitPrice);
                stmt.executeUpdate();
            }

            BigDecimal lineTotal = unitPrice.multiply(BigDecimal.valueOf(quantity));
            totalAmount = totalAmount.add(lineTotal);

            log.info("  INSERT order_item: order={}, product={}, qty={}, price={}",
                    orderId, productId, quantity, unitPrice);
        }

        return totalAmount;
    }
}
