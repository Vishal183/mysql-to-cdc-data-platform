package com.cdcpipeline.producer.simulator;

import com.cdcpipeline.producer.generator.CustomerGenerator;
import com.cdcpipeline.producer.generator.OrderGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class TrafficSimulator {

    private static final Logger log = LoggerFactory.getLogger(TrafficSimulator.class);

    private final DataSource dataSource;
    private final long intervalMs;
    private final Random random = new Random();

    private final CustomerGenerator customerGen = new CustomerGenerator();
    private final OrderGenerator orderGen = new OrderGenerator();

    private final List<Integer> customerIds = new ArrayList<>();
    private final List<Integer> productIds = new ArrayList<>();

    private volatile boolean running = true;

    public TrafficSimulator(DataSource dataSource, long intervalMs) {
        this.dataSource = dataSource;
        this.intervalMs = intervalMs;
    }

    public void run() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received, stopping simulator...");
            running = false;
        }));

        loadExistingIds();

        log.info("=== Traffic Simulator Started ===");
        log.info("Loaded {} customers, {} products from database", customerIds.size(), productIds.size());
        log.info("Simulation interval: {}ms", intervalMs);
        log.info("================================");

        while (running) {
            try {
                executeRandomAction();
                Thread.sleep(intervalMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (SQLException e) {
                log.error("Database error during simulation: {}", e.getMessage());
            }
        }

        log.info("Traffic Simulator stopped.");
    }

    private void loadExistingIds() {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {

            try (ResultSet rs = stmt.executeQuery("SELECT customer_id FROM customers")) {
                while (rs.next()) {
                    customerIds.add(rs.getInt(1));
                }
            }

            try (ResultSet rs = stmt.executeQuery("SELECT product_id FROM products")) {
                while (rs.next()) {
                    productIds.add(rs.getInt(1));
                }
            }

        } catch (SQLException e) {
            log.error("Failed to load existing IDs: {}", e.getMessage());
        }
    }

    private void executeRandomAction() throws SQLException {
        int roll = random.nextInt(100);

        try (Connection conn = dataSource.getConnection()) {
            if (roll < 30) {
                // 30% — create customer
                int id = customerGen.insertCustomer(conn);
                customerIds.add(id);

            } else if (roll < 55) {
                // 25% — create order with items
                orderGen.createOrder(conn, customerIds, productIds);

            } else if (roll < 80) {
                // 25% — update order status
                orderGen.updateOrderStatus(conn);

            } else if (roll < 90) {
                // 10% — update customer info
                if (!customerIds.isEmpty()) {
                    int customerId = customerIds.get(random.nextInt(customerIds.size()));
                    customerGen.updateCustomer(conn, customerId);
                }

            } else {
                // 10% — delete cancelled order
                orderGen.deleteCancelledOrder(conn);
            }
        }
    }
}
