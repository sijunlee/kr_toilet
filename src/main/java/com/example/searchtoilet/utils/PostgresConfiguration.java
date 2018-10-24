package com.example.searchtoilet.utils;

import io.dropwizard.lifecycle.Managed;
import org.jooq.Configuration;
import org.jooq.ConnectionProvider;
import org.jooq.SQLDialect;
import org.jooq.conf.RenderKeywordStyle;
import org.jooq.conf.Settings;
import org.jooq.conf.StatementType;
import org.jooq.impl.DefaultConfiguration;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

public final class PostgresConfiguration implements Managed {

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final Configuration configuration;
    private final int rowLimit;

    public PostgresConfiguration(@Nonnull String connectString,
                                 @Nonnull String database,
                                 @Nonnull String username,
                                 @Nonnull String password,
                                 boolean enableLogging,
                                 int maxIdleConnections,
                                 int maxTotalConnections,
                                 int rowLimit) {
        this.configuration = new DefaultConfiguration()
                .set(SQLDialect.POSTGRES)
                .set(
                        new Settings()
                                .withRenderSchema(false)
                                .withRenderFormatted(true)
                                .withStatementType(StatementType.PREPARED_STATEMENT)
                                .withExecuteLogging(enableLogging)
                                .withRenderKeywordStyle(RenderKeywordStyle.UPPER)
                )
                .set(
                        new PostgresConnectionProvider(
                                Arrays.asList(connectString.split(",")),
                                database,
                                username,
                                password,
                                maxIdleConnections,
                                maxTotalConnections,
                                0.5
                        ));
        this.rowLimit = rowLimit;

    }

    @Nonnull
    public Configuration getConfiguration() {
        if (!running.get()) {
            throw new IllegalStateException("PostgresConfiguration is not running.");
        }
        return configuration;
    }

    public int getRowLimit() {
        return rowLimit;
    }

    @Override
    public void start() throws Exception {
        if (running.getAndSet(true)) {
            throw new IllegalStateException("PostgresConfiguration is already running.");
        }
    }

    @Override
    public void stop() throws Exception {
        if (running.getAndSet(false)) {
            final ConnectionProvider connectionProvider = configuration.connectionProvider();
            if (connectionProvider instanceof PostgresConnectionProvider) {
                final PostgresConnectionProvider cast = (PostgresConnectionProvider) connectionProvider;
                cast.shutdown();
            } else {
                throw new IllegalStateException("ConnectionProvider is not a PostgresConnectionProvider.");
            }
        }
    }

}
