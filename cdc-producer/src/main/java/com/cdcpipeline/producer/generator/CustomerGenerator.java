package com.cdcpipeline.producer.generator;

import net.datafaker.Faker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public class CustomerGenerator {

    private static final Logger log = LoggerFactory.getLogger(CustomerGenerator.class);
    private final Faker faker = new Faker();

    public int insertCustomer(Connection conn) throws SQLException {
        String sql = "INSERT INTO customers (first_name, last_name, email, phone, address) VALUES (?, ?, ?, ?, ?)";

        String firstName = faker.name().firstName();
        String lastName = faker.name().lastName();
        String email = firstName.toLowerCase() + "." + lastName.toLowerCase()
                + "." + faker.number().numberBetween(1, 9999) + "@" + faker.internet().domainName();
        String phone = faker.phoneNumber().cellPhone();
        String address = faker.address().fullAddress();

        try (PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            stmt.setString(1, firstName);
            stmt.setString(2, lastName);
            stmt.setString(3, email);
            stmt.setString(4, phone);
            stmt.setString(5, address);
            stmt.executeUpdate();

            try (ResultSet keys = stmt.getGeneratedKeys()) {
                keys.next();
                int customerId = keys.getInt(1);
                log.info("INSERT customer #{}: {} {} ({})", customerId, firstName, lastName, email);
                return customerId;
            }
        }
    }

    public void updateCustomer(Connection conn, int customerId) throws SQLException {
        String sql = "UPDATE customers SET phone = ?, address = ? WHERE customer_id = ?";
        String newPhone = faker.phoneNumber().cellPhone();
        String newAddress = faker.address().fullAddress();

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, newPhone);
            stmt.setString(2, newAddress);
            stmt.setInt(3, customerId);
            int rows = stmt.executeUpdate();
            if (rows > 0) {
                log.info("UPDATE customer #{}: new phone={}, new address={}", customerId, newPhone, newAddress);
            }
        }
    }
}
