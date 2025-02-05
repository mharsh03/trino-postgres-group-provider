package io.psg.trino.groupprovider.postgres;

import io.trino.spi.Plugin;
import io.trino.spi.security.GroupProviderFactory;

import java.util.Collections;

public final class PostgresGroupProviderPlugin implements Plugin {

    @Override
    public Iterable<GroupProviderFactory> getGroupProviderFactories() {
        return Collections.singletonList(new PostgresGroupProviderFactory());
    }
}