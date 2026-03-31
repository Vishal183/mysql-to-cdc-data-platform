package com.cdcpipeline.producer.generator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

public class ClickstreamGenerator {

    private static final Logger log = LoggerFactory.getLogger(ClickstreamGenerator.class);
    private final Random random = new Random();

    private static final String[] SEARCH_QUERIES = {
        "wireless headphones", "running shoes", "laptop stand", "yoga mat",
        "water bottle", "keyboard", "monitor", "jacket", "dumbbell",
        "usb charger", "desk lamp", "jeans", "t-shirt", "ssd drive"
    };

    public void generateBrowsingSession(Connection conn,
            List<Integer> customerIds, List<Integer> productIds) throws SQLException {

        if (customerIds.isEmpty() || productIds.isEmpty()) {
            log.warn("Cannot generate clickstream: no customers or products available");
            return;
        }

        int customerId = customerIds.get(random.nextInt(customerIds.size()));
        String sessionId = UUID.randomUUID().toString();
        int browseCount = 3 + random.nextInt(6); // 3-8 product views

        // Shuffle products and pick a subset to browse
        List<Integer> shuffled = new ArrayList<>(productIds);
        Collections.shuffle(shuffled, random);
        List<Integer> browsedProducts = shuffled.subList(0, Math.min(browseCount, shuffled.size()));

        String sql = "INSERT INTO clickstream (customer_id, product_id, event_type, search_query, session_id) "
                   + "VALUES (?, ?, ?, ?, ?)";

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            int eventCount = 0;

            // Generate VIEW events for each browsed product
            for (int productId : browsedProducts) {
                stmt.setInt(1, customerId);
                stmt.setInt(2, productId);
                stmt.setString(3, "VIEW");
                stmt.setNull(4, Types.VARCHAR);
                stmt.setString(5, sessionId);
                stmt.addBatch();
                eventCount++;

                // 30% chance of ADD_TO_CART after viewing
                if (random.nextInt(100) < 30) {
                    stmt.setInt(1, customerId);
                    stmt.setInt(2, productId);
                    stmt.setString(3, "ADD_TO_CART");
                    stmt.setNull(4, Types.VARCHAR);
                    stmt.setString(5, sessionId);
                    stmt.addBatch();
                    eventCount++;
                }

                // 10% chance of WISHLIST after viewing
                if (random.nextInt(100) < 10) {
                    stmt.setInt(1, customerId);
                    stmt.setInt(2, productId);
                    stmt.setString(3, "WISHLIST");
                    stmt.setNull(4, Types.VARCHAR);
                    stmt.setString(5, sessionId);
                    stmt.addBatch();
                    eventCount++;
                }
            }

            // 15% chance of a SEARCH event in the session
            if (random.nextInt(100) < 15) {
                String query = SEARCH_QUERIES[random.nextInt(SEARCH_QUERIES.length)];
                stmt.setInt(1, customerId);
                stmt.setNull(2, Types.INTEGER);
                stmt.setString(3, "SEARCH");
                stmt.setString(4, query);
                stmt.setString(5, sessionId);
                stmt.addBatch();
                eventCount++;
            }

            stmt.executeBatch();
            log.info("CLICKSTREAM session {}: customer={}, {} events ({} views)",
                    sessionId.substring(0, 8), customerId, eventCount, browsedProducts.size());
        }
    }
}
