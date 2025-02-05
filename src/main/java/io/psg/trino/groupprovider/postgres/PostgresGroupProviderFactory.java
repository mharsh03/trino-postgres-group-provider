package io.psg.trino.groupprovider.postgres;

import io.trino.spi.security.GroupProvider;
import io.trino.spi.security.GroupProviderFactory;

import java.util.Map;

public class PostgresGroupProviderFactory implements GroupProviderFactory {

    @Override
    public String getName() {
        return "postgres";
    }

    @Override
    public GroupProvider create(Map<String, String> config) {
        String jdbcUrl = config.get("postgres.jdbc-url");
        String dbUser = config.get("postgres.user");
        String dbPassword = config.get("postgres.password");

        // if (config.isEmpty()) {
        //     throw new IllegalArgumentException("this group provider requires configuration properties");
        // }

        if (jdbcUrl == null || dbUser == null || dbPassword == null) {
            throw new IllegalArgumentException("Missing required configuration for PostgreSQL Group Provider");
        }

        return new PostgresGroupProvider(config);
    }
}