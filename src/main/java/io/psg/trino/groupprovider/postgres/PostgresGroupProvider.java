package io.psg.trino.groupprovider.postgres;

import io.trino.spi.security.GroupProvider;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Logger;

public class PostgresGroupProvider implements GroupProvider {
    private static final Logger LOGGER = Logger.getLogger(PostgresGroupProvider.class.getName());

    private static final String GROUP_QUERY = "SELECT group_name FROM user_groups WHERE user_name = ?";
    private final Map<String, Set<String>> groupCache = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final String jdbcUrl;
    private final String dbUser;
    private final String dbPassword;
    private final long refreshIntervalMillis;

    public PostgresGroupProvider(Map<String, String> config) {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("PostgreSQL JDBC driver not found", e);
        }
        
        this.jdbcUrl = config.get("postgres.jdbc-url");
        this.dbUser = config.get("postgres.user");
        this.dbPassword = config.get("postgres.password");
        this.refreshIntervalMillis = Long.parseLong(config.getOrDefault("refresh-interval", "600000")); // Default 5 minutes

        // Initialize periodic cache refresh
        scheduler.scheduleAtFixedRate(this::refreshCache, 0, refreshIntervalMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public Set<String> getGroups(String user) {
        return groupCache.getOrDefault(user, Collections.emptySet());
    }

    private void refreshCache() {
        LOGGER.info("Refreshing group cache from PostgreSQL...");
        try (Connection connection = DriverManager.getConnection(jdbcUrl, dbUser, dbPassword)) {
            PreparedStatement statement = connection.prepareStatement("SELECT DISTINCT user_name FROM user_groups");
            ResultSet resultSet = statement.executeQuery();

            Map<String, Set<String>> newCache = new ConcurrentHashMap<>();
            while (resultSet.next()) {
                String user = resultSet.getString("user_name");
                Set<String> groups = fetchGroupsForUser(connection, user);
                newCache.put(user, groups);
            }
            groupCache.clear();
            groupCache.putAll(newCache);

            LOGGER.info("Group cache successfully refreshed.");
        } catch (SQLException e) {
            LOGGER.severe("Error refreshing group cache: " + e.getMessage());
        }
    }

    private Set<String> fetchGroupsForUser(Connection connection, String user) throws SQLException {
        Set<String> groups = new HashSet<>();
        try (PreparedStatement statement = connection.prepareStatement(GROUP_QUERY)) {
            statement.setString(1, user);
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                groups.add(resultSet.getString("group_name"));
            }
        }
        return groups;
    }

    public void shutdown() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
        }
    }
}

// package com.example.trino.groupprovider.postgres;

// import io.trino.spi.security.GroupProvider;

// import java.sql.*;
// import java.util.Collections;
// import java.util.HashSet;
// import java.util.Set;

// public class PostgresGroupProvider implements GroupProvider {
//     private final String jdbcUrl;
//     private final String dbUser;
//     private final String dbPassword;

//     public PostgresGroupProvider(String jdbcUrl, String dbUser, String dbPassword) {
//         this.jdbcUrl = jdbcUrl;
//         this.dbUser = dbUser;
//         this.dbPassword = dbPassword;
//     }

//     @Override
//     public Set<String> getGroups(String user) {
//         String query = "SELECT group_name FROM user_groups WHERE user_name = ?";
//         try (Connection connection = DriverManager.getConnection(jdbcUrl, dbUser, dbPassword);
//              PreparedStatement statement = connection.prepareStatement(query)) {

//             statement.setString(1, user);
//             ResultSet resultSet = statement.executeQuery();

//             Set<String> groups = new HashSet<>();
//             while (resultSet.next()) {
//                 groups.add(resultSet.getString("group_name"));
//             }
//             return groups;

//         } catch (SQLException e) {
//             throw new RuntimeException("Failed to fetch groups from PostgreSQL for user: " + user, e);
//         }
//     }
// }
