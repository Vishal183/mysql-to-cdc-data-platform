package com.cdcpipeline.producer;

import com.cdcpipeline.producer.config.DatabaseConfig;
import com.cdcpipeline.producer.simulator.TrafficSimulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerApp {

    private static final Logger log = LoggerFactory.getLogger(ProducerApp.class);

    public static void main(String[] args) {
        log.info("Starting E-commerce Traffic Simulator...");

        long intervalMs = Long.parseLong(
                System.getenv().getOrDefault("SIMULATION_INTERVAL_MS", "2000"));

        DatabaseConfig dbConfig = null;
        try {
            dbConfig = waitForDatabase();
            TrafficSimulator simulator = new TrafficSimulator(dbConfig.getDataSource(), intervalMs);
            simulator.run();
        } catch (Exception e) {
            log.error("Fatal error: {}", e.getMessage(), e);
            System.exit(1);
        } finally {
            if (dbConfig != null) {
                dbConfig.close();
            }
        }
    }

    private static DatabaseConfig waitForDatabase() {
        int maxRetries = 30;
        for (int i = 1; i <= maxRetries; i++) {
            try {
                DatabaseConfig config = new DatabaseConfig();
                // Test the connection
                config.getDataSource().getConnection().close();
                log.info("Connected to MySQL successfully.");
                return config;
            } catch (Exception e) {
                log.warn("MySQL not ready (attempt {}/{}): {}", i, maxRetries, e.getMessage());
                if (i == maxRetries) {
                    throw new RuntimeException("Could not connect to MySQL after " + maxRetries + " attempts", e);
                }
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting for MySQL", ie);
                }
            }
        }
        throw new RuntimeException("Unreachable");
    }
}
