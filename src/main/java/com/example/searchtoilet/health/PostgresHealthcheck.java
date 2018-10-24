package com.example.searchtoilet.health;

import com.codahale.metrics.health.HealthCheck;
import com.example.searchtoilet.utils.PostgresConfiguration;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

import javax.annotation.Nonnull;

public class PostgresHealthcheck extends HealthCheck {

    private final PostgresConfiguration config;
    public PostgresHealthcheck(@Nonnull  PostgresConfiguration config) {
        this.config = config;
    }

    @Override
    protected Result check() throws Exception {
        final DSLContext dsl = DSL.using(config.getConfiguration());
        if (dsl.execute("SELECT 1 = 1") >= 0) {
            return Result.healthy();
        } else {
            return Result.unhealthy("Database is unreachable.");
        }
    }
}
